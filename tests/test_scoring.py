"""Tests locaux de la Lambda de scoring (données synthétiques + moto, sans AWS réel)."""
import base64
import importlib.util
import json
import pickle
import random
import sys
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
for p in ("src/common", "src/scoring"):
    if str(ROOT / p) not in sys.path:
        sys.path.insert(0, str(ROOT / p))

import features  # noqa: E402
from init_state import initial_states  # noqa: E402
from state_store import batch_get_states, batch_put_states, from_item, to_item  # noqa: E402

# Deux modules s'appellent handler.py (simulateur et scoring) : on charge celui-ci sous un autre nom.
_spec = importlib.util.spec_from_file_location("scoring_handler", ROOT / "src" / "scoring" / "handler.py")
scoring = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(scoring)

TYPE_ORDER = {"telemetry": 0, "error": 1, "maint": 2}
START = "2015-10-01 00:00:00"


@pytest.fixture
def data_dir(tmp_path):
    """2 machines, mesures du 28/09 au 02/10, erreurs et maintenances (dont 2014 et à l'heure du début)."""
    rng = random.Random(0)
    hours = pd.date_range("2015-09-28 00:00", "2015-10-02 23:00", freq="h")
    pd.DataFrame([
        {"datetime": h.strftime("%Y-%m-%d %H:%M:%S"), "machineID": m,
         "volt": round(rng.uniform(150, 190), 3), "rotate": round(rng.uniform(400, 500), 3),
         "pressure": round(rng.uniform(90, 110), 3), "vibration": round(rng.uniform(35, 45), 3)}
        for h in hours for m in (1, 2)
    ]).to_csv(tmp_path / "PdM_telemetry.csv", index=False)
    pd.DataFrame([
        {"datetime": "2015-09-29 10:00:00", "machineID": 1, "errorID": "error1"},  # hors des 24 h
        {"datetime": "2015-09-30 05:00:00", "machineID": 1, "errorID": "error2"},  # dans les 24 h
        {"datetime": "2015-09-30 23:00:00", "machineID": 2, "errorID": "error4"},
        {"datetime": "2015-10-01 00:00:00", "machineID": 2, "errorID": "error5"},  # à l'heure du début
        {"datetime": "2015-10-01 12:00:00", "machineID": 1, "errorID": "error3"},
    ]).to_csv(tmp_path / "PdM_errors.csv", index=False)
    pd.DataFrame([
        {"datetime": "2014-07-01 06:00:00", "machineID": 1, "comp": "comp1"},
        {"datetime": "2015-09-15 06:00:00", "machineID": 1, "comp": "comp2"},
        {"datetime": "2015-08-01 06:00:00", "machineID": 2, "comp": "comp3"},
        {"datetime": "2015-10-01 00:00:00", "machineID": 1, "comp": "comp4"},  # à l'heure du début
        {"datetime": "2015-10-02 06:00:00", "machineID": 2, "comp": "comp1"},
    ]).to_csv(tmp_path / "PdM_maint.csv", index=False)
    pd.DataFrame([{"machineID": 1, "model": "model3", "age": 18},
                  {"machineID": 2, "model": "model1", "age": 7}]).to_csv(tmp_path / "PdM_machines.csv", index=False)
    return tmp_path


def all_events(data_dir: Path) -> list[dict]:
    """Tous les événements (contrat 1), triés comme le rejeu."""
    tel = pd.read_csv(data_dir / "PdM_telemetry.csv")
    err = pd.read_csv(data_dir / "PdM_errors.csv")
    mnt = pd.read_csv(data_dir / "PdM_maint.csv")
    evs = [{"type": "telemetry", "machineID": int(r.machineID), "datetime": r.datetime,
            **{s: float(getattr(r, s)) for s in features.SENSORS}} for r in tel.itertuples()]
    evs += [{"type": "error", "machineID": int(r.machineID), "datetime": r.datetime, "errorID": r.errorID}
            for r in err.itertuples()]
    evs += [{"type": "maint", "machineID": int(r.machineID), "datetime": r.datetime, "comp": r.comp}
            for r in mnt.itertuples()]
    evs.sort(key=lambda e: (e["datetime"], TYPE_ORDER[e["type"]], e["machineID"]))
    return evs


def continuous_features(data_dir: Path) -> dict:
    """Ce que fait build_dataset.py : historique continu, un calcul après chaque mesure."""
    models = {1: ("model3", 18), 2: ("model1", 7)}
    states = {m: features.new_state(m, *models[m]) for m in models}
    out = {}
    for ev in all_events(data_dir):
        m = ev["machineID"]
        states[m] = features.update_state(states[m], ev)
        if ev["type"] == "telemetry":
            out[(m, ev["datetime"])] = features.compute_features(states[m])
    return out


@pytest.fixture
def capture_predict(monkeypatch):
    """Remplace le modèle : renvoie l'indice de la ligne et garde les vecteurs reçus."""
    seen: list[list[float]] = []

    def fake(rows):
        seen.extend(rows)
        return [0.5] * len(rows)
    monkeypatch.setattr(scoring, "predict", fake)
    return seen


# ------------------------------------------------------------------ state_store

def test_item_roundtrip():
    state = features.new_state(7, "model2", 12, {"comp1": "2015-09-01 06:00:00"})
    state["telemetry_24h"] = [["2015-10-01 00:00:00", 170.25, 450.0, 100.5, 40.0]]
    state["errors_24h"] = [["2015-10-01 00:00:00", "error1"]]
    state.update(last_datetime="2015-10-01 00:00:00", risk=0.125, risk_datetime="2015-10-01 00:00:00", last_alert=None)
    back = from_item(to_item(state))
    assert back == state
    assert isinstance(back["machineID"], int) and isinstance(back["risk"], float)


# ------------------------------------------------------------------ init_state

def test_warm_start_matches_continuous_replay(data_dir, capture_predict):
    """Point clé : avec l'état préchauffé, la Lambda calcule les MÊMES valeurs qu'à l'entraînement."""
    expected = continuous_features(data_dir)
    states = {s["machineID"]: s for s in initial_states(data_dir, START)}
    assert states[1]["last_maint"] == {"comp1": "2014-07-01 06:00:00", "comp2": "2015-09-15 06:00:00"}
    assert states[1]["errors_24h"] == [["2015-09-30 05:00:00", "error2"]]
    assert len(states[1]["telemetry_24h"]) == 24 and states[1]["telemetry_24h"][0][0] == "2015-09-30 00:00:00"

    replay = [e for e in all_events(data_dir) if e["datetime"] >= START]
    scored = scoring.score_batch(states, scoring.group_and_sort(replay))
    got = {(s["machineID"], s["datetime"]): row for s, row in zip(scored, capture_predict)}
    assert len(got) == 2 * 48
    for key, row in got.items():
        assert row == pytest.approx(expected[key]), key


def test_machine_filter(data_dir):
    states = initial_states(data_dir, START, [2])
    assert [s["machineID"] for s in states] == [2]


# ------------------------------------------------------------------ score_batch

def test_duplicates_and_disorder_give_same_features(data_dir, capture_predict):
    """Doublons + désordre dans le lot (renvois Kinesis) : mêmes valeurs qu'un lot propre."""
    replay = [e for e in all_events(data_dir) if e["datetime"] >= START]
    clean_states = {s["machineID"]: s for s in initial_states(data_dir, START)}
    clean = scoring.score_batch(clean_states, scoring.group_and_sort(replay))
    clean_rows = list(capture_predict)
    capture_predict.clear()

    messy = replay + random.Random(1).sample(replay, 40)   # 40 doublons
    random.Random(2).shuffle(messy)
    messy_states = {s["machineID"]: s for s in initial_states(data_dir, START)}
    scored = scoring.score_batch(messy_states, scoring.group_and_sort(messy))

    by_key = {}
    for s, row in zip(scored, capture_predict):
        by_key.setdefault((s["machineID"], s["datetime"]), []).append(row)
    for s, row in zip(clean, clean_rows):
        for dup in by_key[(s["machineID"], s["datetime"])]:
            assert dup == pytest.approx(row)
    assert messy_states == clean_states


def test_older_events_ignored_after_previous_batch(data_dir, capture_predict):
    replay = [e for e in all_events(data_dir) if e["datetime"] >= START]
    states = {s["machineID"]: s for s in initial_states(data_dir, START)}
    scoring.score_batch(states, scoring.group_and_sort(replay))
    capture_predict.clear()
    # Le simulateur renvoie un vieux lot (position non avancée) : rien n'est recalculé.
    old = [e for e in replay if e["datetime"] < "2015-10-02 00:00:00"]
    assert scoring.score_batch(states, scoring.group_and_sort(old)) == []
    assert states[1]["risk_datetime"] == "2015-10-02 23:00:00"


def test_unknown_machine_is_skipped(capture_predict):
    ev = {"type": "telemetry", "machineID": 99, "datetime": START, "volt": 1, "rotate": 1, "pressure": 1, "vibration": 1}
    assert scoring.score_batch({}, scoring.group_and_sort([ev])) == []


def _kinesis(ev) -> dict:
    raw = ev if isinstance(ev, bytes) else (json.dumps(ev) + "\n").encode()
    return {"kinesis": {"data": base64.b64encode(raw).decode(), "sequenceNumber": "1"}}


def test_decode_records_filters():
    good = {"type": "error", "machineID": 1, "datetime": START, "errorID": "error1"}
    recs = [_kinesis(good), _kinesis(b"{pas du json"), _kinesis({"type": "bizarre"}),
            _kinesis({"type": "risk", "machineID": 1}), _kinesis([1, 2])]
    assert scoring.decode_records(recs) == [good]


# ------------------------------------------------------------- Lambda (moto)

class ConstantModel:
    """Faux modèle picklable : risque = 0,99 si une erreur error2 est comptée, sinon 0,01."""

    def predict_proba(self, rows):
        import numpy as np
        idx = features.FEATURE_COLUMNS.index("error2_count_24h")
        p = np.array([0.99 if r[idx] > 0 else 0.01 for r in rows])
        return np.column_stack([1 - p, p])


@pytest.fixture
def aws(data_dir, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("BUCKET", "smartcnc-test")
    monkeypatch.delenv("FAKE_RISK", raising=False)
    with mock_aws():
        scoring._clients.clear()
        scoring._model_cache.clear()
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="smartcnc-test")
        s3.put_object(Bucket="smartcnc-test", Key="models/model.pkl", Body=pickle.dumps(ConstantModel()))
        s3.put_object(Bucket="smartcnc-test", Key="models/columns.json", Body=json.dumps(features.FEATURE_COLUMNS))
        s3.put_object(Bucket="smartcnc-test", Key="models/threshold.json", Body=json.dumps({"threshold": 0.9}))
        ddb = boto3.client("dynamodb")
        ddb.create_table(TableName="machine_state", BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": "machineID", "AttributeType": "N"}],
                         KeySchema=[{"AttributeName": "machineID", "KeyType": "HASH"}])
        batch_put_states(ddb, "machine_state", initial_states(data_dir, START))
        yield ddb
        scoring._clients.clear()
        scoring._model_cache.clear()


def test_lambda_end_to_end(aws, data_dir):
    replay = [e for e in all_events(data_dir) if e["datetime"] >= START]
    first, second = replay[: len(replay) // 2], replay[len(replay) // 2:]

    r1 = scoring.lambda_handler({"Records": [_kinesis(e) for e in first]}, None)
    assert r1["machines"] == 2 and r1["scored"] > 0
    # Machine 1 : error2 du 30/09 05h comptée jusqu'au 01/10 04h -> risque haut puis bas.
    assert r1["above_threshold"] > 0

    r2 = scoring.lambda_handler({"Records": [_kinesis(e) for e in second]}, None)
    assert r1["scored"] + r2["scored"] == 2 * 48

    states = batch_get_states(aws, "machine_state", [1, 2])
    assert states[1]["risk_datetime"] == "2015-10-02 23:00:00"
    assert states[1]["risk"] == pytest.approx(0.01)
    assert states[2]["last_maint"]["comp1"] == "2015-10-02 06:00:00"
    assert len(states[2]["telemetry_24h"]) == 24


def test_lambda_rejects_mismatched_columns(aws):
    boto3.client("s3").put_object(Bucket="smartcnc-test", Key="models/columns.json",
                                  Body=json.dumps(list(reversed(features.FEATURE_COLUMNS))))
    ev = {"type": "telemetry", "machineID": 1, "datetime": START, "volt": 1, "rotate": 1, "pressure": 1, "vibration": 1}
    with pytest.raises(RuntimeError, match="columns.json"):
        scoring.lambda_handler({"Records": [_kinesis(ev)]}, None)


# ------------------------------------------------------ semaine 3 : alertes, archive

def _scored(mid, hours_and_risks):
    return [{"machineID": mid, "datetime": f"2015-10-01 {h:02d}:00:00", "risk": r, "features": [0.0] * 30}
            for h, r in hours_and_risks]


def test_cooldown_12h():
    states = {1: {"last_alert": None}}
    scored = _scored(1, [(0, 0.99), (1, 0.99), (11, 0.99), (12, 0.99), (13, 0.10), (23, 0.99)])
    alerts = scoring.decide_alerts(states, scored, threshold=0.948, cooldown_hours=12)
    assert [a["datetime"][11:13] for a in alerts] == ["00", "12"]   # 01h et 11h : < 12 h ; 23h : 11 h après 12h
    assert states[1]["last_alert"] == "2015-10-01 12:00:00"
    assert [s["alert"] for s in scored] == [True, False, False, True, False, False]


def test_cooldown_continues_from_previous_batch():
    states = {1: {"last_alert": "2015-09-30 20:00:00"}}
    alerts = scoring.decide_alerts(states, _scored(1, [(5, 0.99), (8, 0.99)]), 0.948, 12)
    assert [a["datetime"] for a in alerts] == ["2015-10-01 08:00:00"]


class FailingLambda:
    def __init__(self):
        self.calls = 0

    def invoke(self, **kw):
        self.calls += 1
        raise RuntimeError("Function not found")


def test_agent_failure_restores_last_alert(monkeypatch):
    monkeypatch.setenv("AGENT_FUNCTION", "smartcnc-agent")
    fake = FailingLambda()
    monkeypatch.setitem(scoring._clients, "lambda", fake)
    states = {1: {"last_alert": "2015-09-30 00:00:00", "run_id": "r1"}}
    alerts = scoring.decide_alerts(states, _scored(1, [(5, 0.99)]), 0.948, 12)
    assert scoring.notify_agent(states, alerts, 0.948) == 0
    assert fake.calls == 1
    assert states[1]["last_alert"] == "2015-09-30 00:00:00" and alerts[0]["alert"] is False


def test_agent_payload_has_named_features():
    a = _scored(3, [(11, 0.999)])[0]
    p = scoring.agent_payload(a, 0.948, "r1")
    assert p["machineID"] == 3 and p["run_id"] == "r1" and list(p["features"]) == features.FEATURE_COLUMNS
    json.dumps(p)


def test_emit_metrics_is_valid_emf(capsys):
    scored = _scored(1, [(0, 0.2), (1, 0.99)]) + _scored(2, [(0, 0.5)])
    scoring.emit_metrics(5, scored, 0.948, [scored[1]], {1: {"run_id": "r1"}, 2: {}})
    lines = [json.loads(l) for l in capsys.readouterr().out.splitlines()]
    emf = lines[0]
    assert emf["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "SmartCNC"
    assert emf["AboveThreshold"] == 1 and emf["Alerts"] == 1 and emf["MaxRisk"] == 0.99
    latest = {l["machineID"]: l for l in lines if l["kind"] == "machine_risk"}
    assert latest[1]["datetime"] == "2015-10-01 01:00:00" and set(latest) == {1, 2}
    assert [l["machineID"] for l in lines if l["kind"] == "alert"] == [1]


def test_lambda_archives_risks_and_alerts(aws, data_dir, capsys):
    replay = [e for e in all_events(data_dir) if e["datetime"] >= START]
    r = scoring.lambda_handler({"Records": [_kinesis(e) for e in replay]}, None)
    # Machine 1 : risque haut de 00h à 05h le 01/10 -> une seule alerte (règle des 12 h).
    assert r["alerts"] == 1 and r["agent_calls"] == 0   # pas d'AGENT_FUNCTION : journalisée seulement

    objs = boto3.client("s3").list_objects_v2(Bucket="smartcnc-test", Prefix="stream/risks/")["Contents"]
    assert len(objs) == 1 and objs[0]["Key"].startswith("stream/risks/run=sans-run/2015-10-01/")
    body = boto3.client("s3").get_object(Bucket="smartcnc-test", Key=objs[0]["Key"])["Body"].read().decode()
    rows = [json.loads(l) for l in body.splitlines()]
    assert len(rows) == r["scored"] == 96
    assert sum(x["alert"] for x in rows) == 1 and sum(x["above"] for x in rows) == r["above_threshold"]

    state = batch_get_states(aws, "machine_state", [1])[1]
    assert state["last_alert"] == "2015-10-01 00:00:00"


# ------------------------------------------------------------- vrai modèle de B

MODEL = ROOT / "models" / "model.pkl"


@pytest.mark.skipif(not MODEL.exists(), reason="models/model.pkl absent")
def test_real_model_loads_and_scores(data_dir):
    pytest.importorskip("sklearn")
    model = pickle.loads(MODEL.read_bytes())
    assert json.loads((ROOT / "models" / "columns.json").read_text()) == features.FEATURE_COLUMNS
    state = initial_states(data_dir, START)[0]
    p = model.predict_proba([features.compute_features(state)])[0][1]
    assert 0.0 <= p <= 1.0
