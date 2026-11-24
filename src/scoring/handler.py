"""Lambda de scoring : Kinesis -> état 24 h par machine -> risque -> machine_state.

Pour chaque lot Kinesis :
1. décode les événements, ignore (et journalise) les types inconnus ou les JSON invalides ;
2. les groupe par machine et les trie par (datetime, telemetry < error < maint) : règle 1 de B ;
3. lit l'état des machines concernées (une lecture groupée) ;
4. applique features.update_state à chaque événement ; après chaque mesure ACCEPTÉE,
   calcule compute_features (règle 2 de B : un risque par mesure) ;
5. calcule tous les risques du lot en un seul appel au modèle ;
6. semaine 3 : règle des 12 h, appel asynchrone de l'agent (contrat 4) ;
7. réécrit l'état une seule fois par machine et par lot ;
8. semaine 3 : archive les risques dans S3 (stream/risks/, lu par Athena) et publie
   les métriques du tableau de bord CloudWatch (format EMF, via les journaux).

Variables d'environnement :
    BUCKET          bucket contenant models/ et stream/
    MODEL_PREFIX    défaut "models/"
    STATE_TABLE     défaut "machine_state"
    ARCHIVE_PREFIX  défaut "stream/risks/" ; vide = pas d'archive
    AGENT_FUNCTION  nom de la Lambda agent (B) ; vide = alerte seulement journalisée
    FAKE_RISK       si défini (ex. "0.1"), pas de modèle : risque fixe (tests)
"""
from __future__ import annotations

import base64
import json
import logging
import os
import pickle
import time
import uuid
from datetime import datetime

import boto3

from features import FEATURE_COLUMNS, compute_features, update_state
from state_store import batch_get_states, batch_put_states

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TYPE_ORDER = {"telemetry": 0, "error": 1, "maint": 2}
KNOWN_TYPES = set(TYPE_ORDER) | {"risk", "ticket"}   # risk/ticket : semaine 3, ignorés ici

_clients: dict = {}
_model_cache: dict = {}


def _client(name: str):
    if name not in _clients:
        _clients[name] = boto3.client(name)
    return _clients[name]


# ------------------------------------------------------------------ modèle

def load_model() -> dict:
    """Charge model.pkl, columns.json et threshold.json une fois par conteneur."""
    if _model_cache:
        return _model_cache
    fake = os.environ.get("FAKE_RISK")
    if fake is not None:
        _model_cache.update(model=None, threshold=float(os.environ.get("FAKE_THRESHOLD", "0.948")),
                            cooldown_hours=12.0, fake_risk=float(fake))
        return _model_cache

    bucket, prefix = os.environ["BUCKET"], os.environ.get("MODEL_PREFIX", "models/")
    s3 = _client("s3")

    def read(name: str) -> bytes:
        return s3.get_object(Bucket=bucket, Key=prefix + name)["Body"].read()

    columns = json.loads(read("columns.json"))
    if columns != FEATURE_COLUMNS:
        # Sécurité : le modèle et features.py doivent parler des mêmes 30 colonnes, dans le même ordre.
        raise RuntimeError("columns.json ne correspond pas à features.FEATURE_COLUMNS")
    meta = json.loads(read("threshold.json"))
    threshold = meta["threshold"]
    model = pickle.loads(read("model.pkl"))
    _model_cache.update(model=model, threshold=float(threshold),
                        cooldown_hours=float(meta.get("cooldown_hours", 12)), fake_risk=None)
    logger.info("Modèle chargé depuis s3://%s/%s (seuil %.3f)", bucket, prefix, threshold)
    return _model_cache


def predict(rows: list[list[float]]) -> list[float]:
    m = load_model()
    if not rows:
        return []
    if m["model"] is None:
        return [m["fake_risk"]] * len(rows)
    return [float(p) for p in m["model"].predict_proba(rows)[:, 1]]


# ----------------------------------------------------------------- événements

def decode_records(records: list[dict]) -> list[dict]:
    """Enregistrements Kinesis -> événements valides (contrat 1)."""
    events = []
    for rec in records:
        try:
            ev = json.loads(base64.b64decode(rec["kinesis"]["data"]))
        except (ValueError, KeyError, TypeError):
            logger.warning("Enregistrement illisible ignoré : %s", rec.get("kinesis", {}).get("sequenceNumber"))
            continue
        if not isinstance(ev, dict) or ev.get("type") not in KNOWN_TYPES:
            logger.warning("Type inconnu ignoré : %r", ev.get("type") if isinstance(ev, dict) else ev)
            continue
        if ev["type"] in TYPE_ORDER:
            events.append(ev)
    return events


def group_and_sort(events: list[dict]) -> dict[int, list[dict]]:
    by_machine: dict[int, list[dict]] = {}
    for ev in events:
        by_machine.setdefault(int(ev["machineID"]), []).append(ev)
    for evs in by_machine.values():
        evs.sort(key=lambda e: (e["datetime"], TYPE_ORDER[e["type"]]))
    return by_machine


def score_batch(states: dict[int, dict], by_machine: dict[int, list[dict]]) -> list[dict]:
    """Met à jour les états en place et renvoie un risque par mesure acceptée.

    Renvoie [{"machineID", "datetime", "risk"}, ...] (sert aussi pour l'archive de la semaine 3).
    """
    pending: list[tuple[int, str]] = []
    rows: list[list[float]] = []
    for mid, evs in by_machine.items():
        state = states.get(mid)
        if state is None:
            logger.error("Machine %s absente de machine_state : lancer 'smartcnc.py load-machines'", mid)
            continue
        for ev in evs:
            before = state["last_datetime"]
            state = update_state(state, ev)
            accepted = before is None or ev["datetime"] >= before
            if ev["type"] == "telemetry" and accepted:
                rows.append(compute_features(state))
                pending.append((mid, ev["datetime"]))
        states[mid] = state

    risks = predict(rows)
    scored = [{"machineID": mid, "datetime": dt, "risk": r, "features": x}
              for (mid, dt), r, x in zip(pending, risks, rows)]
    for s in scored:  # le dernier risque de chaque machine est conservé dans son état
        st = states[s["machineID"]]
        if st.get("risk_datetime") is None or s["datetime"] >= st["risk_datetime"]:
            st["risk"], st["risk_datetime"] = s["risk"], s["datetime"]
    return scored


# ------------------------------------------------------------ alertes (semaine 3)

def _hours_between(a: str, b: str) -> float:
    fmt = "%Y-%m-%d %H:%M:%S"
    return (datetime.strptime(b, fmt) - datetime.strptime(a, fmt)).total_seconds() / 3600


def decide_alerts(states: dict[int, dict], scored: list[dict], threshold: float, cooldown_hours: float) -> list[dict]:
    """Règle des 12 h (contrat 4) : au plus une alerte par machine toutes les `cooldown_hours`
    heures SIMULÉES. Met à jour last_alert dans l'état et marque s["alert"] = True."""
    alerts = []
    for s in sorted(scored, key=lambda r: (r["machineID"], r["datetime"])):
        s["alert"] = False
        if s["risk"] < threshold:
            continue
        st = states[s["machineID"]]
        last = st.get("last_alert")
        if last is None or _hours_between(last, s["datetime"]) >= cooldown_hours:
            s["alert"] = True
            s["previous_alert"] = last
            st["last_alert"] = s["datetime"]
            alerts.append(s)
    return alerts


def agent_payload(alert: dict, threshold: float, run_id) -> dict:
    return {
        "machineID": alert["machineID"], "datetime": alert["datetime"], "risk": alert["risk"],
        "threshold": threshold, "run_id": run_id,
        "features": dict(zip(FEATURE_COLUMNS, alert["features"])),
    }


def notify_agent(states: dict[int, dict], alerts: list[dict], threshold: float) -> int:
    """Appelle l'agent (asynchrone) pour chaque alerte. En cas d'échec, last_alert est
    remis à sa valeur précédente pour que l'alerte soit retentée à l'heure suivante."""
    fn = os.environ.get("AGENT_FUNCTION", "")
    sent = 0
    for a in alerts:
        st = states[a["machineID"]]
        if not fn:
            continue  # agent pas encore déployé : alerte seulement journalisée
        try:
            _client("lambda").invoke(FunctionName=fn, InvocationType="Event",
                                     Payload=json.dumps(agent_payload(a, threshold, st.get("run_id"))).encode())
            sent += 1
        except Exception:  # noqa: BLE001 — on ne bloque jamais le scoring à cause de l'agent
            logger.exception("Appel de l'agent impossible pour la machine %s", a["machineID"])
            st["last_alert"] = a["previous_alert"]
            a["alert"] = False
    return sent


# ------------------------------------------------------------- archive et métriques

def archive_risks(states: dict[int, dict], scored: list[dict], threshold: float) -> str | None:
    """Un fichier JSONL par lot dans stream/risks/run=<run_id>/<jour simulé>/ (lu par Athena)."""
    prefix = os.environ.get("ARCHIVE_PREFIX", "stream/risks/")
    if not prefix or not scored:
        return None
    run_id = states[scored[0]["machineID"]].get("run_id") or "sans-run"
    lines = [json.dumps({
        "run_id": states[s["machineID"]].get("run_id") or "sans-run",
        "machineID": s["machineID"], "datetime": s["datetime"], "risk": round(s["risk"], 6),
        "threshold": threshold, "above": s["risk"] >= threshold, "alert": bool(s.get("alert")),
    }) for s in scored]
    day = min(s["datetime"] for s in scored)[:10]
    key = f"{prefix}run={run_id}/{day}/{uuid.uuid4().hex}.jsonl"
    try:
        _client("s3").put_object(Bucket=os.environ["BUCKET"], Key=key, Body=("\n".join(lines) + "\n").encode())
    except Exception:  # noqa: BLE001 — l'archive ne doit pas faire rejouer le lot
        logger.exception("Archive des risques impossible (%s)", key)
        return None
    return key


def emit_metrics(n_events: int, scored: list[dict], threshold: float, alerts: list[dict], states: dict) -> None:
    """Journaux JSON lus par le tableau de bord CloudWatch.

    - une ligne EMF : CloudWatch en tire des métriques (namespace SmartCNC), sans appel d'API ;
    - une ligne par machine (dernier risque) et par alerte : lues par Logs Insights.
    """
    above = sum(1 for s in scored if s["risk"] >= threshold)
    print(json.dumps({
        "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": [{
            "Namespace": "SmartCNC", "Dimensions": [[]],
            "Metrics": [{"Name": "EventsProcessed", "Unit": "Count"}, {"Name": "RisksComputed", "Unit": "Count"},
                        {"Name": "AboveThreshold", "Unit": "Count"}, {"Name": "Alerts", "Unit": "Count"},
                        {"Name": "MaxRisk", "Unit": "None"}],
        }]},
        "kind": "batch", "EventsProcessed": n_events, "RisksComputed": len(scored),
        "AboveThreshold": above, "Alerts": len(alerts),
        "MaxRisk": max((s["risk"] for s in scored), default=0.0),
    }))
    latest: dict[int, dict] = {}
    for s in scored:
        if s["machineID"] not in latest or s["datetime"] > latest[s["machineID"]]["datetime"]:
            latest[s["machineID"]] = s
    for s in latest.values():
        print(json.dumps({"kind": "machine_risk", "machineID": s["machineID"], "datetime": s["datetime"],
                          "risk": round(s["risk"], 4)}))
    for a in alerts:
        print(json.dumps({"kind": "alert", "machineID": a["machineID"], "datetime": a["datetime"],
                          "risk": round(a["risk"], 4), "run_id": states[a["machineID"]].get("run_id"),
                          "agent": bool(os.environ.get("AGENT_FUNCTION")) and bool(a.get("alert"))}))


# --------------------------------------------------------------------- Lambda

def lambda_handler(event, context):
    table = os.environ.get("STATE_TABLE", "machine_state")
    events = decode_records(event.get("Records", []))
    by_machine = group_and_sort(events)
    if not by_machine:
        return {"events": 0, "scored": 0}

    ddb = _client("dynamodb")
    states = batch_get_states(ddb, table, by_machine.keys())
    scored = score_batch(states, by_machine)

    model = load_model()
    threshold = model["threshold"]
    alerts = decide_alerts(states, scored, threshold, model["cooldown_hours"])
    sent = notify_agent(states, alerts, threshold)
    alerts = [a for a in alerts if a["alert"]]

    touched = [states[m] for m in by_machine if m in states]
    batch_put_states(ddb, table, touched)   # après les alertes : last_alert est enregistré

    archive_risks(states, scored, threshold)
    emit_metrics(len(events), scored, threshold, alerts, states)
    above = sum(1 for s in scored if s["risk"] >= threshold)
    logger.info("Lot : %d événements, %d machines, %d risques, %d au-dessus du seuil, %d alertes (%d envoyées à l'agent)",
                len(events), len(touched), len(scored), above, len(alerts), sent)
    return {"events": len(events), "machines": len(touched), "scored": len(scored),
            "above_threshold": above, "alerts": len(alerts), "agent_calls": sent}
