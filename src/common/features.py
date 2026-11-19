"""Caractéristiques SmartCNC AI : contrat 2 (contracts/features.md).

Un seul fichier, partagé par l'entraînement (training/build_dataset.py) et la Lambda
de scoring : les deux calculent donc exactement les mêmes valeurs.

Python pur, sans pandas ni numpy : léger et rapide dans la Lambda.

Utilisation dans la Lambda de scoring, pour chaque événement d'une machine,
dans l'ordre (datetime, puis telemetry < error < maint) :

    state = update_state(state, event)
    if event["type"] == "telemetry":
        x = compute_features(state)        # une prédiction par heure
        risk = model.predict_proba([x])[0][1]

L'état est un dict sérialisable en JSON (dates en texte "YYYY-MM-DD HH:MM:SS").
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SENSORS = ("volt", "rotate", "pressure", "vibration")
ERRORS = ("error1", "error2", "error3", "error4", "error5")
COMPONENTS = ("comp1", "comp2", "comp3", "comp4")
MODELS = ("model1", "model2", "model3", "model4")
SHORT_HOURS = 3
LONG_HOURS = 24
# Début de PdM_maint.csv : référence pour un composant sans maintenance connue.
MAINT_ORIGIN = "2014-06-01 00:00:00"

FEATURE_COLUMNS: list[str] = (
    [f"{s}_{stat}_{h}h" for s in SENSORS for stat in ("mean", "std") for h in (SHORT_HOURS, LONG_HOURS)]
    + [f"{e}_count_24h" for e in ERRORS]
    + [f"{c}_days_since" for c in COMPONENTS]
    + [f"model_{m}" for m in MODELS]
    + ["age"]
)
assert len(FEATURE_COLUMNS) == 30


def _parse(dt: str) -> datetime:
    return datetime.strptime(dt, DATETIME_FORMAT)


def _fmt(dt: datetime) -> str:
    return dt.strftime(DATETIME_FORMAT)


# ------------------------------------------------------------------- état

def new_state(machine_id: int, model: str, age: int, last_maint: dict | None = None) -> dict:
    """État vide d'une machine (fiche de PdM_machines.csv).

    last_maint : {"comp1": "2015-09-12 06:00:00", ...}, la dernière maintenance de chaque
    composant AVANT le début du rejeu (voir last_maint_before).
    """
    if model not in MODELS:
        raise ValueError(f"modèle inconnu : {model!r}")
    return {
        "machineID": int(machine_id),
        "model": model,
        "age": int(age),
        "telemetry_24h": [],   # [[datetime, volt, rotate, pressure, vibration], ...] trié
        "errors_24h": [],      # [[datetime, errorID], ...] trié
        "last_maint": dict(last_maint or {}),
        "last_datetime": None,
    }


def last_maint_before(maint_rows, machine_id: int, before: str) -> dict:
    """Dernière maintenance de chaque composant d'une machine, strictement avant `before`.

    maint_rows : itérable de (datetime, machineID, comp), ex. les lignes de PdM_maint.csv.
    Sert à initialiser machine_state avant le rejeu (règle du contrat 2).
    """
    last: dict = {}
    for dt, mid, comp in maint_rows:
        if int(mid) == int(machine_id) and dt < before and dt > last.get(comp, ""):
            last[comp] = dt
    return last


def update_state(state: dict, event: dict) -> dict:
    """Ajoute un événement (contrat 1) à l'état et ne garde que les 24 dernières heures.

    - Un événement plus ancien que le dernier traité est ignoré (contrat 1).
    - Doublons : une mesure déjà reçue pour la même heure est remplacée ;
      une erreur déjà reçue (même heure, même errorID) n'est pas recomptée.
    """
    dt = event["datetime"]
    if state["last_datetime"] is not None and dt < state["last_datetime"]:
        return state
    kind = event["type"]

    if kind == "telemetry":
        row = [dt] + [float(event[s]) for s in SENSORS]
        tel = state["telemetry_24h"]
        if tel and tel[-1][0] == dt:
            tel[-1] = row
        else:
            tel.append(row)
    elif kind == "error":
        err = [dt, event["errorID"]]
        if err not in state["errors_24h"]:
            state["errors_24h"].append(err)
    elif kind == "maint":
        comp = event["comp"]
        if dt > state["last_maint"].get(comp, ""):
            state["last_maint"][comp] = dt
    else:
        return state  # type inconnu : ignoré (la Lambda le journalise)

    state["last_datetime"] = dt
    now = _parse(dt)
    # Mesures : les 24 dernières heures, heure courante comprise (t-23h … t).
    tel_from = _fmt(now - timedelta(hours=LONG_HOURS - 1))
    state["telemetry_24h"] = [r for r in state["telemetry_24h"] if r[0] >= tel_from]
    # Erreurs : on garde t-24h … t, compute_features ne compte que [t-24h, t[.
    err_from = _fmt(now - timedelta(hours=LONG_HOURS))
    state["errors_24h"] = [e for e in state["errors_24h"] if e[0] >= err_from]
    return state


# ------------------------------------------------------- caractéristiques

def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n   # écart-type de population (ddof=0)
    return mean, math.sqrt(var)


def compute_features(state: dict) -> list[float]:
    """Vecteur de 30 valeurs, dans l'ordre de FEATURE_COLUMNS, pour l'heure t
    de la dernière mesure reçue.

    - Capteurs : moyenne et écart-type des mesures de [t-2h, t] (3h) et [t-23h, t] (24h).
      Avec une seule mesure, l'écart-type vaut 0.
    - Erreurs : nombre d'erreurs de chaque type dans [t-24h, t[ (heure t exclue,
      donc le résultat ne dépend pas de l'ordre d'arrivée dans l'heure t).
    - Maintenance : jours depuis la dernière maintenance reçue de chaque composant
      (depuis le 2014-06-01 si aucune). D'où la règle d'appel : juste après la mesure
      de l'heure t, AVANT les erreurs et maintenances de la même heure (ordre du rejeu).
      Ainsi une réparation à l'heure d'une panne n'est jamais vue avant cette panne.
    - Machine : modèle en one-hot, âge en années.
    """
    tel = state["telemetry_24h"]
    if not tel:
        raise ValueError(f"machine {state.get('machineID')} : aucune mesure, impossible de calculer")
    t_str = tel[-1][0]
    t = _parse(t_str)
    short_from = _fmt(t - timedelta(hours=SHORT_HOURS - 1))
    long_from = _fmt(t - timedelta(hours=LONG_HOURS - 1))
    windows = {
        SHORT_HOURS: [r for r in tel if r[0] >= short_from],
        LONG_HOURS: [r for r in tel if r[0] >= long_from],
    }

    sensor_stats = {}
    for h, rows in windows.items():
        for i, s in enumerate(SENSORS, start=1):
            sensor_stats[(s, h)] = _mean_std([r[i] for r in rows])
    features = []
    for s in SENSORS:
        for stat in (0, 1):            # mean, std
            for h in (SHORT_HOURS, LONG_HOURS):
                features.append(sensor_stats[(s, h)][stat])

    err_from = _fmt(t - timedelta(hours=LONG_HOURS))
    counts = dict.fromkeys(ERRORS, 0)
    for dt, eid in state["errors_24h"]:
        if err_from <= dt < t_str and eid in counts:
            counts[eid] += 1
    features += [float(counts[e]) for e in ERRORS]

    for c in COMPONENTS:
        last = state["last_maint"].get(c) or MAINT_ORIGIN
        features.append(max(0.0, (t - _parse(last)).total_seconds() / 86400.0))

    features += [1.0 if state["model"] == m else 0.0 for m in MODELS]
    features.append(float(state["age"]))
    return features