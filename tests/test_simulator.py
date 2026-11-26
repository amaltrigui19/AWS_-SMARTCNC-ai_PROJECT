"""Tests locaux du rejeu et de la Lambda simulateur (données synthétiques + moto)."""
import json
import sys
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "simulator"))

import importlib.util  # noqa: E402

import prepare_replay  # noqa: E402

# Trois modules s'appellent handler.py (simulateur, scoring, agent) : on charge celui du
# simulateur sous un nom à lui, sinon pytest peut nous donner celui d'un autre test.
_spec = importlib.util.spec_from_file_location("simulator_handler", ROOT / "src" / "simulator" / "handler.py")
handler = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(handler)


@pytest.fixture
def data_dir(tmp_path):
    """3 machines, du 30/09 00h au 03/10 23h, format de date 'M/D/YYYY H:MM:SS AM' pour tester le parsing."""
    hours = pd.date_range("2015-09-30 00:00", "2015-10-03 23:00", freq="h")
    rows = [
        {"datetime": h.strftime("%m/%d/%Y %I:%M:%S %p"), "machineID": m,
         "volt": 170.0 + m, "rotate": 450.0, "pressure": 100.0, "vibration": 40.0}
        for h in hours for m in (1, 2, 3)
    ]
    pd.DataFrame(rows).to_csv(tmp_path / "PdM_telemetry.csv", index=False)
    pd.DataFrame([
        {"datetime": "2015-09-30 10:00:00", "machineID": 1, "errorID": "error1"},  # avant la période
        {"datetime": "2015-10-01 06:00:00", "machineID": 2, "errorID": "error3"},
        {"datetime": "2015-10-02 23:00:00", "machineID": 3, "errorID": "error5"},
    ]).to_csv(tmp_path / "PdM_errors.csv", index=False)
    pd.DataFrame([
        {"datetime": "2015-10-01 06:00:00", "machineID": 2, "comp": "comp4"},
    ]).to_csv(tmp_path / "PdM_maint.csv", index=False)
    # Fichier de pannes volontairement invalide : s'il était lu, le test planterait.
    (tmp_path / "PdM_failures.csv").write_text("not,a,valid\ncsv")
    return tmp_path


# ------------------------------------------------------------- prepare_replay

def test_parse_machines():
    assert prepare_replay.parse_machines("all") is None
    assert prepare_replay.parse_machines("1-3,7") == [1, 2, 3, 7]


def test_build_events_period_order_and_format(data_dir):
    ev = prepare_replay.build_events(data_dir, "2015-10-01", "2015-10-03", None)
    dts = ev["datetime"]
    assert dts.min() == pd.Timestamp("2015-10-01 00:00") and dts.max() == pd.Timestamp("2015-10-02 23:00")
    assert set(ev["type"]) == {"telemetry", "error", "maint"}
    assert len(ev) == 48 * 3 + 2 + 1
    # Tri : datetime puis telemetry < error < maint
    at6 = ev[dts == pd.Timestamp("2015-10-01 06:00")]["type"].tolist()
    assert at6 == ["telemetry"] * 3 + ["error", "maint"]
    first = json.loads(ev["json"].iloc[0])
    assert first == {"type": "telemetry", "machineID": 1, "datetime": "2015-10-01 00:00:00",
                     "volt": 171.0, "rotate": 450.0, "pressure": 100.0, "vibration": 40.0}
    err = json.loads(ev[ev["type"] == "error"]["json"].iloc[0])
    assert err == {"type": "error", "machineID": 2, "datetime": "2015-10-01 06:00:00", "errorID": "error3"}


def test_machine_filter(data_dir):
    ev = prepare_replay.build_events(data_dir, "2015-10-01", "2015-10-03", [2])
    assert set(ev["machineID"]) == {2}


def test_write_replay(data_dir, tmp_path):
    ev = prepare_replay.build_events(data_dir, "2015-10-01", "2015-10-03", None)
    out = tmp_path / "out"
    m = prepare_replay.write_replay(ev, out, "2015-10-01", "2015-10-03", None)
    assert m["files"] == ["2015-10-01.jsonl", "2015-10-02.jsonl"]
    assert m["start"] == "2015-10-01 00:00:00" and m["end"] == "2015-10-03 00:00:00"
    assert m["total"] == len(ev)
    lines = (out / "2015-10-01.jsonl").read_text().splitlines()
    assert all(json.loads(l)["datetime"].startswith("2015-10-01") for l in lines)


# -------------------------------------------------------------- send_records

class FakeKinesis:
    """Refuse les N premiers enregistrements du premier appel."""

    def __init__(self, reject_first=0, always_reject=False):
        self.reject_first, self.always_reject, self.received, self.calls = reject_first, always_reject, [], 0

    def put_records(self, StreamName, Records):
        self.calls += 1
        out = []
        for i, r in enumerate(Records):
            if self.always_reject or (self.calls == 1 and i < self.reject_first):
                out.append({"ErrorCode": "ProvisionedThroughputExceededException"})
            else:
                self.received.append(r)
                out.append({"SequenceNumber": "1", "ShardId": "shardId-0"})
        return {"Records": out, "FailedRecordCount": sum("ErrorCode" in o for o in out)}


def _recs(n):
    return [{"Data": b"x\n", "PartitionKey": str(i)} for i in range(n)]


def test_send_records_chunks_and_retries():
    fake = FakeKinesis(reject_first=10)
    sent = handler.send_records(fake, "s", _recs(1200), sleep=lambda s: None)
    assert sent == 1200 and len(fake.received) == 1200
    assert fake.calls == 4  # 500 (10 refusés) + 10 renvoyés + 500 + 200


def test_send_records_gives_up():
    with pytest.raises(RuntimeError):
        handler.send_records(FakeKinesis(always_reject=True), "s", _recs(3), sleep=lambda s: None)


def test_send_records_respects_rate():
    slept = []
    handler.send_records(FakeKinesis(), "s", _recs(1000), rps=500, sleep=slept.append, clock=lambda: 0.0)
    assert sum(slept) == pytest.approx(2.0)  # 1000 enregistrements à 500/s


# ---------------------------------------------------------- Lambda de bout en bout

@pytest.fixture
def aws(data_dir, tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("BUCKET", "smartcnc-test")
    monkeypatch.setenv("REPLAY_PREFIX", "replay/test")  # sans "/" final : doit être toléré
    monkeypatch.setenv("HOURS_PER_BATCH", "30")
    monkeypatch.setenv("MAX_RECORDS_PER_SECOND", "100000")
    with mock_aws():
        handler._clients.clear()
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="smartcnc-test")
        ev = prepare_replay.build_events(data_dir, "2015-10-01", "2015-10-03", None)
        out = tmp_path / "replay"
        prepare_replay.write_replay(ev, out, "2015-10-01", "2015-10-03", None)
        for f in out.iterdir():
            s3.upload_file(str(f), "smartcnc-test", f"replay/test/{f.name}")
        boto3.client("dynamodb").create_table(
            TableName="replay_state", BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}])
        kin = boto3.client("kinesis")
        kin.create_stream(StreamName="smartcnc-measurements", ShardCount=1)
        yield {"kinesis": kin, "total": len(ev)}
        handler._clients.clear()


def _read_stream(kin):
    shard = kin.describe_stream(StreamName="smartcnc-measurements")["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(StreamName="smartcnc-measurements", ShardId=shard,
                                ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    return kin.get_records(ShardIterator=it, Limit=10000)["Records"]


def test_lambda_replays_whole_period(aws):
    r1 = handler.lambda_handler({}, None)
    assert r1["status"] == "ok" and r1["from"] == "2015-10-01 00:00:00" and r1["to"] == "2015-10-02 06:00:00"
    assert r1["sent"] == 30 * 3 + 2  # 30 h × 3 machines + erreur + maint de 06:00

    r2 = handler.lambda_handler({}, None)
    assert r2["to"] == "2015-10-03 00:00:00"  # dernier lot tronqué à la fin du manifest
    assert r1["sent"] + r2["sent"] == aws["total"]

    assert handler.lambda_handler({}, None)["status"] == "termine"

    records = _read_stream(aws["kinesis"])
    assert len(records) == aws["total"]
    assert all(r["Data"].endswith(b"\n") for r in records)  # JSON ligne par ligne pour Firehose/Athena
    first = json.loads(records[0]["Data"])
    assert records[0]["PartitionKey"] == str(first["machineID"])
    dts = [json.loads(r["Data"])["datetime"] for r in records]
    assert dts == sorted(dts)
    assert not any(json.loads(r["Data"])["type"] == "failure" for r in records)


def test_lambda_position_conflict(aws):
    handler.lambda_handler({}, None)
    # Un autre appel a avancé la position entre la lecture et l'écriture.
    real_get = handler.get_position
    handler.get_position = lambda cfg: "2015-10-01 00:00:00"
    try:
        assert handler.lambda_handler({}, None)["status"] == "conflit"
    finally:
        handler.get_position = real_get
