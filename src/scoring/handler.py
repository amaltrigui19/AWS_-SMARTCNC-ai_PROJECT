"""Lambda de scoring : Kinesis -> état 24 h par machine -> risque -> machine_state.

Pour chaque lot Kinesis :
1. décode les événements, ignore (et journalise) les types inconnus ou les JSON invalides ;
2. les groupe par machine et les trie par (datetime, telemetry < error < maint) : règle 1 de B ;
3. lit l'état des machines concernées (une lecture groupée) ;
4. applique features.update_state à chaque événement ; après chaque mesure ACCEPTÉE,
   calcule compute_features (règle 2 de B : un risque par mesure) ;
5. calcule tous les risques du lot en un seul appel au modèle ;
6. réécrit l'état une seule fois par machine et par lot.

Variables d'environnement :
    BUCKET         bucket contenant models/
    MODEL_PREFIX   défaut "models/"
    STATE_TABLE    défaut "machine_state"
    FAKE_RISK      si défini (ex. "0.1"), pas de modèle : risque fixe (tests)
"""
from __future__ import annotations

import base64
import json
import logging
import os
import pickle

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
                            fake_risk=float(fake))
        return _model_cache

    bucket, prefix = os.environ["BUCKET"], os.environ.get("MODEL_PREFIX", "models/")
    s3 = _client("s3")

    def read(name: str) -> bytes:
        return s3.get_object(Bucket=bucket, Key=prefix + name)["Body"].read()

    columns = json.loads(read("columns.json"))
    if columns != FEATURE_COLUMNS:
        # Sécurité : le modèle et features.py doivent parler des mêmes 30 colonnes, dans le même ordre.
        raise RuntimeError("columns.json ne correspond pas à features.FEATURE_COLUMNS")
    threshold = json.loads(read("threshold.json"))["threshold"]
    model = pickle.loads(read("model.pkl"))
    _model_cache.update(model=model, threshold=float(threshold), fake_risk=None)
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
    scored = [{"machineID": mid, "datetime": dt, "risk": r} for (mid, dt), r in zip(pending, risks)]
    for s in scored:  # le dernier risque de chaque machine est conservé dans son état
        st = states[s["machineID"]]
        if st.get("risk_datetime") is None or s["datetime"] >= st["risk_datetime"]:
            st["risk"], st["risk_datetime"] = s["risk"], s["datetime"]
    return scored


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
    touched = [states[m] for m in by_machine if m in states]
    batch_put_states(ddb, table, touched)

    threshold = load_model()["threshold"]
    above = [s for s in scored if s["risk"] >= threshold]
    for s in above:
        logger.info("RISQUE ÉLEVÉ machine %s à %s : %.3f", s["machineID"], s["datetime"], s["risk"])
    logger.info("Lot : %d événements, %d machines, %d risques calculés, %d au-dessus du seuil",
                len(events), len(touched), len(scored), len(above))
    return {"events": len(events), "machines": len(touched), "scored": len(scored), "above_threshold": len(above)}
