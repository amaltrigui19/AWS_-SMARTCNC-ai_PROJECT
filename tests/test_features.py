"""Tests de src/common/features.py (contrat 2) et des cibles de training/build_dataset.py."""
import json
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "common"))
sys.path.insert(0, str(ROOT / "training"))

import features as F  # noqa: E402
import build_dataset as B  # noqa: E402

START = datetime(2015, 10, 1, 0, 0, 0)


def ts(h: int) -> str:
    return (START + timedelta(hours=h)).strftime(F.DATETIME_FORMAT)


def tel(h: int, volt=170.0, rotate=450.0, pressure=100.0, vibration=40.0) -> dict:
    return {"type": "telemetry", "datetime": ts(h), "volt": volt, "rotate": rotate,
            "pressure": pressure, "vibration": vibration}


def fresh(**kw):
    return F.new_state(1, kw.get("model", "model3"), kw.get("age", 10), kw.get("last_maint"))


def test_columns():
    assert len(F.FEATURE_COLUMNS) == 30 and len(set(F.FEATURE_COLUMNS)) == 30
    assert F.FEATURE_COLUMNS[0] == "volt_mean_3h" and F.FEATURE_COLUMNS[-1] == "age"


def test_vector_length_and_order():
    s = fresh(model="model2", age=7)
    s = F.update_state(s, tel(0))
    x = F.compute_features(s)
    row = dict(zip(F.FEATURE_COLUMNS, x))
    assert len(x) == 30 and row["model_model2"] == 1.0 and row["model_model3"] == 0.0 and row["age"] == 7.0
    assert row["volt_mean_3h"] == 170.0


def test_std_is_zero_with_one_measure_and_population_std():
    s = fresh()
    s = F.update_state(s, tel(0, volt=100))
    assert dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))["volt_std_3h"] == 0.0
    s = F.update_state(s, tel(1, volt=200))
    row = dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))
    assert row["volt_mean_3h"] == 150.0 and math.isclose(row["volt_std_3h"], 50.0)  # ddof=0


def test_windows_3h_and_24h():
    s = fresh()
    for h in range(30):
        s = F.update_state(s, tel(h, volt=float(h)))
    row = dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))
    assert row["volt_mean_3h"] == pytest.approx((27 + 28 + 29) / 3)
    assert row["volt_mean_24h"] == pytest.approx(sum(range(6, 30)) / 24)
    assert len(s["telemetry_24h"]) == 24


def test_error_counts_exclude_current_hour_and_older_than_24h():
    s = fresh()
    s = F.update_state(s, tel(0))
    s = F.update_state(s, {"type": "error", "datetime": ts(0), "errorID": "error1"})
    for h in range(1, 30):
        s = F.update_state(s, tel(h))
        if h == 10:
            s = F.update_state(s, {"type": "error", "datetime": ts(10), "errorID": "error3"})
        if h == 20:
            s = F.update_state(s, {"type": "error", "datetime": ts(20), "errorID": "error3"})
    s = F.update_state(s, tel(30))
    row = dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))
    assert row["error1_count_24h"] == 0.0   # heure 0 : plus de 24 h avant l'heure 30
    assert row["error3_count_24h"] == 2.0
    # Une erreur de l'heure t n'est pas comptée à l'heure t
    s2 = F.update_state(fresh(), tel(5))
    s2 = F.update_state(s2, {"type": "error", "datetime": ts(5), "errorID": "error2"})
    assert dict(zip(F.FEATURE_COLUMNS, F.compute_features(s2)))["error2_count_24h"] == 0.0


def test_duplicates_and_late_events():
    s = fresh()
    s = F.update_state(s, tel(0, volt=100))
    s = F.update_state(s, tel(1, volt=120))
    s = F.update_state(s, tel(1, volt=120))          # doublon : remplacé, pas ajouté
    assert len(s["telemetry_24h"]) == 2
    s = F.update_state(s, tel(0, volt=999))          # plus ancien que le dernier : ignoré
    assert [r[1] for r in s["telemetry_24h"]] == [100.0, 120.0]
    e = {"type": "error", "datetime": ts(1), "errorID": "error4"}
    s = F.update_state(F.update_state(s, e), e)      # erreur reçue deux fois : comptée une fois
    assert len(s["errors_24h"]) == 1


def test_unknown_event_type_is_ignored():
    s = fresh()
    s = F.update_state(s, tel(0))
    before = json.dumps(s)
    assert json.dumps(F.update_state(s, {"type": "risk", "datetime": ts(1)})) == before


def test_maintenance_days_since():
    s = fresh(last_maint={"comp1": "2015-09-21 00:00:00"})       # 10 jours avant le 1er octobre
    s = F.update_state(s, tel(0))
    row = dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))
    assert row["comp1_days_since"] == pytest.approx(10.0)
    # comp2 inconnu : depuis le 2014-06-01
    assert row["comp2_days_since"] == pytest.approx((START - datetime(2014, 6, 1)).days)
    s = F.update_state(s, {"type": "maint", "datetime": ts(0), "comp": "comp1"})
    s = F.update_state(s, tel(24))
    assert dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))["comp1_days_since"] == pytest.approx(1.0)


def test_repair_in_same_hour_as_failure_is_not_seen_before_it():
    """Dans l'ordre du rejeu (mesure, puis maintenance), la ligne de l'heure t ne voit pas
    la réparation de l'heure t : days_since n'est jamais remis à 0 juste avant la prédiction."""
    s = fresh(last_maint={"comp2": "2015-09-21 00:00:00"})
    s = F.update_state(s, tel(0))
    x_before = dict(zip(F.FEATURE_COLUMNS, F.compute_features(s)))
    s = F.update_state(s, {"type": "maint", "datetime": ts(0), "comp": "comp2"})
    assert x_before["comp2_days_since"] == pytest.approx(10.0)


def test_last_maint_before():
    rows = [("2015-09-01 06:00:00", 1, "comp1"), ("2015-09-20 06:00:00", 1, "comp1"),
            ("2015-10-01 00:00:00", 1, "comp1"), ("2015-09-10 06:00:00", 2, "comp1")]
    assert F.last_maint_before(rows, 1, "2015-10-01 00:00:00") == {"comp1": "2015-09-20 06:00:00"}


def test_state_is_json_serializable():
    s = F.update_state(fresh(), tel(0))
    s = F.update_state(s, {"type": "error", "datetime": ts(0), "errorID": "error1"})
    assert json.loads(json.dumps(s)) == s


def test_no_measure_raises():
    with pytest.raises(ValueError):
        F.compute_features(fresh())


# ---------------------------------------------------------------- cibles

def test_labels():
    fails = [ts(30), ts(40)]
    comps = {ts(30): ["comp2"], ts(40): ["comp1", "comp4"]}
    y, hrs, comp = B.labels_for(ts(6), fails, comps)           # panne à exactement t+24h : incluse
    assert (y, hrs, comp) == (1, 24.0, "comp2")
    y, hrs, comp = B.labels_for(ts(5), fails, comps)           # panne à t+25h : hors horizon
    assert (y, hrs, comp) == (0, 25.0, "none")
    y, _, comp = B.labels_for(ts(20), fails, comps)            # deux pannes dans l'horizon
    assert (y, comp) == (1, "comp1|comp2|comp4")
    y, _, comp = B.labels_for(ts(30), fails, comps)            # panne à l'heure t : pas comptée
    assert (y, comp) == (1, "comp1|comp4")
    y, hrs, comp = B.labels_for(ts(41), fails, comps)          # plus aucune panne
    assert y == 0 and math.isnan(hrs) and comp == "none"
    assert B.labels_for(ts(0), [], {})[0] == 0                 # machine sans panne
