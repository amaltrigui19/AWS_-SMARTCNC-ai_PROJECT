"""Entraîne le modèle de risque de panne à 24 h et exporte les 3 fichiers pour la Lambda de scoring.

Découpage PAR DATE (jamais au hasard) :
    entraînement  1er janvier  -> 31 août       (la dernière journée avant la coupure est retirée :
    validation    1er septembre -> 30 septembre  ses cibles regardent 24 h dans la période suivante)
    test          1er octobre   -> 31 décembre   (la même période que le rejeu de la démo)

La validation sert à choisir le seuil d'alerte ; le test ne sert qu'à mesurer.

Sorties dans --out (à déposer dans models/ du bucket d'Eya) :
    model.pkl        modèle scikit-learn (même version que la Lambda : requirements.txt)
    columns.json     FEATURE_COLUMNS, dans l'ordre attendu par le modèle
    threshold.json   seuil d'alerte (+ rappel des résultats)
    metrics.json     résultats détaillés (pour le rapport)

Exemple :
    python training/train.py --dataset dataset.parquet --data s3://smartcnc-381491831586/raw --out models_out
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "common"))
from features import FEATURE_COLUMNS  # noqa: E402

VAL_START = pd.Timestamp("2015-09-01")
TEST_START = pd.Timestamp("2015-10-01")
EMBARGO = pd.Timedelta(hours=24)
COOLDOWN = pd.Timedelta(hours=12)   # une alerte par machine toutes les 12 h (règle de l'agent)


def split(df: pd.DataFrame):
    train = df[df["datetime"] < VAL_START - EMBARGO]
    val = df[(df["datetime"] >= VAL_START) & (df["datetime"] < TEST_START - EMBARGO)]
    test = df[df["datetime"] >= TEST_START]
    return train, val, test


def fit(train: pd.DataFrame, seed: int = 0) -> HistGradientBoostingClassifier:
    model = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.08, max_leaf_nodes=31, min_samples_leaf=200,
        l2_regularization=1.0, class_weight="balanced",
        early_stopping=True, validation_fraction=0.1, n_iter_no_change=20, random_state=seed,
    )
    return model.fit(train[FEATURE_COLUMNS].to_numpy(), train["failure_24h"].to_numpy())


def risk(model, df: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(df[FEATURE_COLUMNS].to_numpy())[:, 1]


def best_threshold(y: np.ndarray, p: np.ndarray) -> float:
    """Seuil qui maximise F1 sur la validation."""
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))])


def row_metrics(y: np.ndarray, p: np.ndarray, thr: float) -> dict:
    pred = p >= thr
    tp = int((pred & (y == 1)).sum())
    return {
        "pr_auc": float(average_precision_score(y, p)),
        "roc_auc": float(roc_auc_score(y, p)),
        "baseline_pr_auc": float(y.mean()),          # PR-AUC d'un modèle qui devine au hasard
        "precision": tp / max(int(pred.sum()), 1),
        "recall": tp / max(int((y == 1).sum()), 1),
    }


def alert_metrics(df: pd.DataFrame, p: np.ndarray, thr: float, failures: pd.DataFrame) -> dict:
    """Ce que verra la démo : alertes avec 12 h d'attente par machine, et délai d'anticipation."""
    d = df[["machineID", "datetime", "failure_24h"]].copy()
    d["p"] = p
    alerts = []   # (machineID, datetime, juste ?)
    for mid, g in d.sort_values("datetime").groupby("machineID"):
        last = None
        for t, hit, pr in zip(g["datetime"], g["failure_24h"], g["p"]):
            if pr >= thr and (last is None or t - last >= COOLDOWN):
                alerts.append((mid, t, int(hit)))
                last = t
    alerts_df = pd.DataFrame(alerts, columns=["machineID", "datetime", "hit"])

    t0, t1 = df["datetime"].min(), df["datetime"].max()
    fl = failures[(failures["datetime"] > t0) & (failures["datetime"] <= t1 + pd.Timedelta(hours=1))]
    fl = fl.drop_duplicates(["machineID", "datetime"])
    lead_hours, detected = [], 0
    for mid, ft in zip(fl["machineID"], fl["datetime"]):
        a = alerts_df[(alerts_df["machineID"] == mid) & (alerts_df["datetime"] < ft)
                      & (alerts_df["datetime"] >= ft - pd.Timedelta(hours=24))]
        if len(a):
            detected += 1
            lead_hours.append((ft - a["datetime"].min()).total_seconds() / 3600)
    n_days = (t1 - t0).total_seconds() / 86400
    n_machines = d["machineID"].nunique()
    return {
        "alerts": int(len(alerts_df)),
        "alerts_per_machine_per_week": len(alerts_df) / n_machines / (n_days / 7),
        "alert_precision": float(alerts_df["hit"].mean()) if len(alerts_df) else 0.0,
        "failures": int(len(fl)),
        "failures_detected_24h_before": detected,
        "failure_recall": detected / max(len(fl), 1),
        "lead_time_hours_median": float(np.median(lead_hours)) if lead_hours else None,
        "lead_time_hours_p25_p75": [float(np.percentile(lead_hours, 25)), float(np.percentile(lead_hours, 75))]
        if lead_hours else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Entraîne le modèle et exporte model.pkl, columns.json, threshold.json.")
    ap.add_argument("--dataset", default="dataset.parquet", help="Sortie de training/build_dataset.py")
    ap.add_argument("--data", default="data", help="Dossier (ou préfixe s3://) contenant PdM_failures.csv")
    ap.add_argument("--out", default="models_out", help="Dossier de sortie")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    t0 = time.time()
    df = pd.read_parquet(args.dataset)
    failures = pd.read_csv(f"{args.data.rstrip('/')}/PdM_failures.csv")
    failures["datetime"] = pd.to_datetime(failures["datetime"], format="mixed")

    train, val, test = split(df)
    print(f"train {len(train):,} ({train['failure_24h'].mean():.2%}) | "
          f"val {len(val):,} ({val['failure_24h'].mean():.2%}) | test {len(test):,} ({test['failure_24h'].mean():.2%})")

    model = fit(train, args.seed)
    print(f"{model.n_iter_} arbres, {time.time() - t0:.0f} s")

    p_val = risk(model, val)
    thr = best_threshold(val["failure_24h"].to_numpy(), p_val)
    p_test = risk(model, test)
    y_test = test["failure_24h"].to_numpy()

    metrics = {
        "threshold": thr,
        "validation": row_metrics(val["failure_24h"].to_numpy(), p_val, thr),
        "test_rows": row_metrics(y_test, p_test, thr),
        "test_alerts": alert_metrics(test, p_test, thr, failures),
        "split": {"train_end": str(VAL_START - EMBARGO), "val": [str(VAL_START), str(TEST_START - EMBARGO)],
                  "test_start": str(TEST_START)},
    }

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "model.pkl", "wb") as f:
        pickle.dump(model, f)
    (out / "columns.json").write_text(json.dumps(FEATURE_COLUMNS, indent=2))
    (out / "threshold.json").write_text(json.dumps(
        {"threshold": thr, "cooldown_hours": 12, "horizon_hours": 24,
         "precision_val": metrics["validation"]["precision"], "recall_val": metrics["validation"]["recall"]}, indent=2))
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2))

    r, a = metrics["test_rows"], metrics["test_alerts"]
    print(f"\nseuil {thr:.3f}")
    print(f"TEST par heure  : PR-AUC {r['pr_auc']:.3f} (hasard {r['baseline_pr_auc']:.3f}) | "
          f"précision {r['precision']:.2f} | rappel {r['recall']:.2f}")
    print(f"TEST par alerte : {a['alerts']} alertes ({a['alerts_per_machine_per_week']:.2f} par machine et par semaine), "
          f"{a['alert_precision']:.0%} justes")
    print(f"TEST pannes     : {a['failures_detected_24h_before']}/{a['failures']} annoncées à l'avance "
          f"({a['failure_recall']:.0%}), délai médian {a['lead_time_hours_median']} h")
    print(f"-> {out}/ ({time.time() - t0:.0f} s)")


if __name__ == "__main__":
    main()
