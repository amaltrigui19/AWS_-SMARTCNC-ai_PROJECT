"""Construit le fichier de rejeu à partir des CSV Azure PdM (exécuté sur le PC).

Sortie : un fichier JSONL par jour (<out>/AAAA-MM-JJ.jsonl) + manifest.json.
Chaque ligne est un événement au format du contrat 1 (contracts/events.md).

Règles appliquées :
- PdM_failures.csv n'est JAMAIS lu : les pannes restent la vérité terrain.
- Seule la période [start, end[ est rejouée (période de test par défaut : octobre à décembre).
- Tri par datetime, puis type (telemetry, error, maint), puis machineID.

Exemples :
    python src/simulator/prepare_replay.py --data data --out data/replay/full
    python src/simulator/prepare_replay.py --data data --out data/replay/test \
        --machines 1-10 --start 2015-10-01 --end 2015-10-15
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TYPE_ORDER = {"telemetry": 0, "error": 1, "maint": 2}
SENSORS = ["volt", "rotate", "pressure", "vibration"]


def parse_machines(spec: str) -> list[int] | None:
    """'all' -> None ; '1-10' -> [1..10] ; '1,5,7' -> [1,5,7]."""
    if spec.strip().lower() == "all":
        return None
    ids: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-")
            ids.update(range(int(lo), int(hi) + 1))
        elif part:
            ids.add(int(part))
    return sorted(ids)


def _load(path: Path, start: pd.Timestamp, end: pd.Timestamp, machines: list[int] | None) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["datetime"] = pd.to_datetime(df["datetime"], format="mixed")
    df["machineID"] = df["machineID"].astype(int)
    mask = (df["datetime"] >= start) & (df["datetime"] < end)
    if machines is not None:
        mask &= df["machineID"].isin(machines)
    return df.loc[mask].copy()


def build_events(data_dir: Path, start: str, end: str, machines: list[int] | None) -> pd.DataFrame:
    """Renvoie un DataFrame trié avec les colonnes datetime, type, machineID, json."""
    t0, t1 = pd.Timestamp(start), pd.Timestamp(end)
    frames = []

    tel = _load(data_dir / "PdM_telemetry.csv", t0, t1, machines)
    tel["type"] = "telemetry"
    tel["json"] = [
        json.dumps({"type": "telemetry", "machineID": int(m), "datetime": d.strftime(DATETIME_FORMAT),
                    **{s: float(v) for s, v in zip(SENSORS, values)}})
        for m, d, values in zip(tel["machineID"], tel["datetime"], tel[SENSORS].to_numpy())
    ]
    frames.append(tel)

    err = _load(data_dir / "PdM_errors.csv", t0, t1, machines)
    err["type"] = "error"
    err["json"] = [
        json.dumps({"type": "error", "machineID": int(m), "datetime": d.strftime(DATETIME_FORMAT), "errorID": str(e)})
        for m, d, e in zip(err["machineID"], err["datetime"], err["errorID"])
    ]
    frames.append(err)

    mnt = _load(data_dir / "PdM_maint.csv", t0, t1, machines)
    mnt["type"] = "maint"
    mnt["json"] = [
        json.dumps({"type": "maint", "machineID": int(m), "datetime": d.strftime(DATETIME_FORMAT), "comp": str(c)})
        for m, d, c in zip(mnt["machineID"], mnt["datetime"], mnt["comp"])
    ]
    frames.append(mnt)

    events = pd.concat([f[["datetime", "type", "machineID", "json"]] for f in frames], ignore_index=True)
    events["type_order"] = events["type"].map(TYPE_ORDER)
    events = events.sort_values(["datetime", "type_order", "machineID"], kind="stable")
    return events.drop(columns="type_order").reset_index(drop=True)


def write_replay(events: pd.DataFrame, out_dir: Path, start: str, end: str, machines: list[int] | None) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.jsonl"):
        old.unlink()
    files = []
    for day, group in events.groupby(events["datetime"].dt.strftime("%Y-%m-%d"), sort=True):
        name = f"{day}.jsonl"
        (out_dir / name).write_text("\n".join(group["json"]) + "\n", encoding="utf-8")
        files.append(name)
    manifest = {
        "start": pd.Timestamp(start).strftime(DATETIME_FORMAT),
        "end": pd.Timestamp(end).strftime(DATETIME_FORMAT),
        "machines": machines if machines is not None else "all",
        "counts": {k: int(v) for k, v in events["type"].value_counts().items()},
        "total": int(len(events)),
        "files": files,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    p = argparse.ArgumentParser(description="Construit le fichier de rejeu (pannes exclues).")
    p.add_argument("--data", default="data", help="Dossier contenant les CSV PdM_*.csv")
    p.add_argument("--out", default="data/replay/full", help="Dossier de sortie")
    p.add_argument("--start", default="2015-10-01", help="Début inclus (AAAA-MM-JJ)")
    p.add_argument("--end", default="2016-01-01", help="Fin exclue (AAAA-MM-JJ)")
    p.add_argument("--machines", default="all", help="'all', '1-10' ou '1,5,7'")
    args = p.parse_args()

    machines = parse_machines(args.machines)
    events = build_events(Path(args.data), args.start, args.end, machines)
    manifest = write_replay(events, Path(args.out), args.start, args.end, machines)
    print(json.dumps({k: v for k, v in manifest.items() if k != "files"}, indent=2))
    print(f"{len(manifest['files'])} fichiers écrits dans {args.out}")


if __name__ == "__main__":
    main()
