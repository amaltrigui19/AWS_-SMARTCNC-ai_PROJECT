"""Construit le jeu d'entraînement : une ligne par machine et par heure.

Les caractéristiques sont calculées en rejouant les événements dans src/common/features.py,
exactement comme la Lambda de scoring : aucun écart possible entre entraînement et production.

Cibles (heure t) :
- failure_24h       : 1 si une panne survient dans ]t, t+24h], sinon 0
- hours_to_failure  : heures jusqu'à la prochaine panne (vide s'il n'y en a pas)
- failure_comp      : composant(s) en panne dans ]t, t+24h], ex. "comp2" ou "comp1|comp2", sinon "none"

Exemples :
    python training/build_dataset.py --data data --out data/dataset.parquet
    python training/build_dataset.py --data s3://smartcnc-381491831586/raw --out dataset.parquet   # SageMaker
    python training/build_dataset.py --data data --out data/dataset_test.parquet --machines 1-10
"""
from __future__ import annotations

import argparse
import bisect
import sys
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "common"))
from features import (  # noqa: E402
    DATETIME_FORMAT, FEATURE_COLUMNS, SENSORS, compute_features, new_state, update_state, _parse,
)

TYPE_ORDER = {"telemetry": 0, "error": 1, "maint": 2}   # même ordre que le rejeu (prepare_replay.py)
HORIZON_HOURS = 24


def _read(data: str, name: str) -> pd.DataFrame:
    path = f"{data.rstrip('/')}/{name}"
    df = pd.read_csv(path)
    if "datetime" in df:
        df["datetime"] = pd.to_datetime(df["datetime"], format="mixed").dt.strftime(DATETIME_FORMAT)
    df["machineID"] = df["machineID"].astype(int)
    return df


def _parse_machines(spec: str) -> set[int] | None:
    if spec == "all":
        return None
    ids: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        ids.update(range(int(lo), int(hi or lo) + 1))
    return ids


def machine_events(tel: pd.DataFrame, err: pd.DataFrame, mnt: pd.DataFrame) -> list[dict]:
    """Événements d'une machine au format du contrat 1, triés (datetime, type)."""
    events = [
        {"type": "telemetry", "datetime": d, **dict(zip(SENSORS, vals))}
        for d, *vals in tel[["datetime", *SENSORS]].itertuples(index=False)
    ]
    events += [{"type": "error", "datetime": d, "errorID": e}
               for d, e in err[["datetime", "errorID"]].itertuples(index=False)]
    events += [{"type": "maint", "datetime": d, "comp": c}
               for d, c in mnt[["datetime", "comp"]].itertuples(index=False)]
    events.sort(key=lambda ev: (ev["datetime"], TYPE_ORDER[ev["type"]]))
    return events


def labels_for(t: str, fail_times: list[str], fail_comps: dict[str, list[str]]) -> tuple[int, float, str]:
    """Cibles de l'heure t à partir des pannes de la machine (triées)."""
    i = bisect.bisect_right(fail_times, t)          # première panne strictement après t
    if i == len(fail_times):
        return 0, float("nan"), "none"
    t_dt = _parse(t)
    hours = (_parse(fail_times[i]) - t_dt).total_seconds() / 3600
    if hours > HORIZON_HOURS:
        return 0, hours, "none"
    limit = (t_dt + timedelta(hours=HORIZON_HOURS)).strftime(DATETIME_FORMAT)
    comps = sorted({c for ft in fail_times[i:] if ft <= limit for c in fail_comps[ft]})
    return 1, hours, "|".join(comps)


def build(data: str, machines: set[int] | None = None) -> pd.DataFrame:
    tel, err, mnt = _read(data, "PdM_telemetry.csv"), _read(data, "PdM_errors.csv"), _read(data, "PdM_maint.csv")
    fail, mach = _read(data, "PdM_failures.csv"), _read(data, "PdM_machines.csv")
    ids = sorted(mach["machineID"]) if machines is None else sorted(machines)

    groups = {name: dict(tuple(df.groupby("machineID"))) for name, df in
              (("tel", tel), ("err", err), ("mnt", mnt), ("fail", fail))}
    empty = pd.DataFrame(columns=["datetime", "machineID", "errorID", "comp", "failure"])
    rows = []
    for mid in ids:
        info = mach.loc[mach["machineID"] == mid].iloc[0]
        state = new_state(mid, info["model"], info["age"])
        f = groups["fail"].get(mid, empty)
        fail_comps: dict[str, list[str]] = {}
        for d, c in f[["datetime", "failure"]].itertuples(index=False):
            fail_comps.setdefault(d, []).append(c)
        fail_times = sorted(fail_comps)

        events = machine_events(groups["tel"][mid], groups["err"].get(mid, empty), groups["mnt"].get(mid, empty))
        for ev in events:
            state = update_state(state, ev)
            if ev["type"] == "telemetry":        # même moment de calcul que la Lambda
                t = ev["datetime"]
                rows.append([mid, t, *compute_features(state), *labels_for(t, fail_times, fail_comps)])

    cols = ["machineID", "datetime", *FEATURE_COLUMNS, "failure_24h", "hours_to_failure", "failure_comp"]
    df = pd.DataFrame(rows, columns=cols)
    df["datetime"] = pd.to_datetime(df["datetime"], format=DATETIME_FORMAT)
    return df


def main() -> None:
    p = argparse.ArgumentParser(description="Construit le jeu d'entraînement (une ligne par machine et par heure).")
    p.add_argument("--data", default="data", help="Dossier (ou préfixe s3://) contenant les 5 CSV")
    p.add_argument("--out", default="data/dataset.parquet", help="Fichier de sortie (.parquet ou .csv.gz)")
    p.add_argument("--machines", default="all", help="'all', '1-10' ou '1,5,7'")
    args = p.parse_args()

    t0 = time.time()
    df = build(args.data, _parse_machines(args.machines))
    if args.out.endswith(".parquet"):
        df.to_parquet(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)
    print(f"{len(df):,} lignes, {df['machineID'].nunique()} machines, "
          f"{df['failure_24h'].mean():.2%} de positifs, {time.time() - t0:.0f} s -> {args.out}")


if __name__ == "__main__":
    main()
