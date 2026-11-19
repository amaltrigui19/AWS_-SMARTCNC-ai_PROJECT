"""État initial de chaque machine au début du rejeu (exécuté sur le PC).

Pour que la Lambda calcule dès la première heure les mêmes valeurs qu'à l'entraînement
(où l'historique était continu), on « préchauffe » l'état avec features.update_state :
    - toutes les maintenances AVANT le début du rejeu (last_maint, contrat 2) ;
    - les mesures et erreurs des 24 h qui précèdent le début du rejeu.
On obtient exactement l'état que build_dataset.py avait au même instant.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src" / "common"))

from features import DATETIME_FORMAT, SENSORS, new_state, update_state  # noqa: E402

TYPE_ORDER = {"telemetry": 0, "error": 1, "maint": 2}


def _read(path: Path, machines: set[int] | None, until: str, since: str | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["datetime"] = pd.to_datetime(df["datetime"], format="mixed").dt.strftime(DATETIME_FORMAT)
    df["machineID"] = df["machineID"].astype(int)
    mask = df["datetime"] < until
    if since is not None:
        mask &= df["datetime"] >= since
    if machines is not None:
        mask &= df["machineID"].isin(machines)
    return df.loc[mask]


def initial_states(data_dir: Path, start: str, machines: list[int] | None = None) -> list[dict]:
    """États préchauffés de chaque machine, prêts à écrire dans machine_state.

    start : début du rejeu, "YYYY-MM-DD HH:MM:SS" (champ "start" du manifest).
    """
    wanted = set(machines) if machines is not None else None
    warm_from = (datetime.strptime(start, DATETIME_FORMAT) - timedelta(hours=24)).strftime(DATETIME_FORMAT)

    mach = pd.read_csv(data_dir / "PdM_machines.csv")
    mach["machineID"] = mach["machineID"].astype(int)
    if wanted is not None:
        mach = mach[mach["machineID"].isin(wanted)]

    tel = _read(data_dir / "PdM_telemetry.csv", wanted, start, warm_from)
    err = _read(data_dir / "PdM_errors.csv", wanted, start, warm_from)
    mnt = _read(data_dir / "PdM_maint.csv", wanted, start)   # tout l'historique avant le début

    events: dict[int, list[dict]] = {}
    for d, m, *vals in tel[["datetime", "machineID", *SENSORS]].itertuples(index=False):
        events.setdefault(m, []).append({"type": "telemetry", "datetime": d, **dict(zip(SENSORS, vals))})
    for d, m, e in err[["datetime", "machineID", "errorID"]].itertuples(index=False):
        events.setdefault(m, []).append({"type": "error", "datetime": d, "errorID": e})
    for d, m, c in mnt[["datetime", "machineID", "comp"]].itertuples(index=False):
        events.setdefault(m, []).append({"type": "maint", "datetime": d, "comp": c})

    states = []
    for mid, model, age in mach[["machineID", "model", "age"]].itertuples(index=False):
        state = new_state(int(mid), str(model), int(age))
        for ev in sorted(events.get(mid, []), key=lambda e: (e["datetime"], TYPE_ORDER[e["type"]])):
            state = update_state(state, ev)
        state["risk"] = None
        state["risk_datetime"] = None
        state["last_alert"] = None
        states.append(state)
    return states
