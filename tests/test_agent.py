"""Tests de la Lambda agent (moto pour DynamoDB, SNS, S3 ; Bedrock simulé)."""
import json
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "agent"))
import handler  # noqa: E402

EVENT = {
    "machineID": 3, "datetime": "2015-10-03 07:00:00", "risk": 0.9993, "threshold": 0.948,
    "run_id": "20261124T183059",
    "features": {"error2_count_24h": 1.0, "comp1_days_since": 10.0, "comp2_days_since": 45.0,
                 "comp3_days_since": 5.0, "comp4_days_since": 20.0, "volt_mean_3h": 171.0,
                 "volt_mean_24h": 170.0, "rotate_mean_3h": 450.0, "rotate_mean_24h": 449.0,
                 "pressure_mean_3h": 100.0, "pressure_mean_24h": 100.0,
                 "vibration_mean_3h": 40.0, "vibration_mean_24h": 40.0},
}


@pytest.fixture
def aws(monkeypatch):
    with mock_aws():
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        monkeypatch.setenv("MODEL_ID", "amazon.nova-lite-v1:0")
        for k in ("LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL"):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("BUCKET", "bkt")
        monkeypatch.setenv("TICKETS_TABLE", "tickets")
        monkeypatch.setenv("STATE_TABLE", "machine_state")
        handler._clients.clear()
        ddb = boto3.resource("dynamodb")
        ddb.create_table(TableName="machine_state", BillingMode="PAY_PER_REQUEST",
                         KeySchema=[{"AttributeName": "machineID", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "machineID", "AttributeType": "N"}])
        ddb.create_table(TableName="tickets", BillingMode="PAY_PER_REQUEST",
                         KeySchema=[{"AttributeName": "ticketID", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "ticketID", "AttributeType": "S"}])
        ddb.Table("machine_state").put_item(Item={
            "machineID": 3, "model": "model2", "age": 8, "risk": handler.Decimal("0.9993"),
            "errors_24h": [["2015-10-02 07:00:00", "error2"]],
            "last_maint": {"comp1": "2015-09-01 06:00:00", "comp2": "2015-08-20 06:00:00"},
            "last_datetime": "2015-10-03 07:00:00",
        })
        boto3.client("s3").create_bucket(Bucket="bkt")
        sns = boto3.client("sns")
        topic = sns.create_topic(Name="alerts")["TopicArn"]
        monkeypatch.setenv("TOPIC_ARN", topic)
        sqs = boto3.client("sqs")
        q = sqs.create_queue(QueueName="mails")["QueueUrl"]
        sns.subscribe(TopicArn=topic, Protocol="sqs",
                      Endpoint=sqs.get_queue_attributes(QueueUrl=q, AttributeNames=["QueueArn"])
                      ["Attributes"]["QueueArn"])
        yield {"q": q, "sqs": sqs, "ddb": ddb}


def _mails(aws):
    msgs = aws["sqs"].receive_message(QueueUrl=aws["q"], MaxNumberOfMessages=10).get("Messages", [])
    return [json.loads(m["Body"]) for m in msgs]


def test_tools_read_state(aws):
    state = handler._get_state(3)
    assert handler.tool_recent_errors(state) == [{"datetime": "2015-10-02 07:00:00", "error": "error2"}]
    assert handler.tool_maintenance_history(state)["comp3"] is None
    assert handler.tool_machine_sheet(state) == {"model": "model2", "age": 8}
    assert handler._get_state(99) == {}


def test_full_flow(aws, monkeypatch):
    monkeypatch.setattr(handler, "ask_bedrock", lambda p: 'Voici : {"suspected_comp": "comp2", '
                        '"recommendation": "Vérifier comp2."}')
    out = handler.lambda_handler(EVENT)
    assert out["status"] == "ok" and out["suspected_comp"] == "comp2"
    item = aws["ddb"].Table("tickets").scan()["Items"][0]
    assert item["machineID"] == 3 and item["status"] == "ouvert" and item["suspected_comp"] == "comp2"
    assert item["created_at"] == "2015-10-03 07:00:00"
    mails = _mails(aws)
    assert len(mails) == 1 and "Machine : 3" in mails[0]["Message"]
    obj = boto3.client("s3").get_object(
        Bucket="bkt", Key=f"stream/tickets/run=20261124T183059/{item['ticketID']}.json")
    row = json.loads(obj["Body"].read())
    assert row["run_id"] == "20261124T183059" and row["risk"] == 0.9993


def test_state_never_modified(aws, monkeypatch):
    monkeypatch.setattr(handler, "ask_bedrock", lambda p: '{"suspected_comp": "comp1", "recommendation": "x"}')
    before = aws["ddb"].Table("machine_state").scan()["Items"]
    handler.lambda_handler(EVENT)
    assert aws["ddb"].Table("machine_state").scan()["Items"] == before


def test_retry_does_not_duplicate(aws, monkeypatch):
    monkeypatch.setattr(handler, "ask_bedrock", lambda p: '{"suspected_comp": "comp2", "recommendation": "x"}')
    handler.lambda_handler(EVENT)
    assert handler.lambda_handler(EVENT)["status"] == "doublon"
    assert len(aws["ddb"].Table("tickets").scan()["Items"]) == 1
    assert len(_mails(aws)) == 1


def test_bedrock_failure_falls_back(aws, monkeypatch):
    def boom(prompt):
        raise RuntimeError("AccessDenied")
    monkeypatch.setattr(handler, "ask_bedrock", boom)
    out = handler.lambda_handler(EVENT)
    assert out["status"] == "ok" and out["suspected_comp"] == "comp2"  # comp2 : 45 j sans maintenance
    assert len(_mails(aws)) == 1


def test_no_model_id_uses_rules_without_bedrock(aws, monkeypatch):
    monkeypatch.setenv("MODEL_ID", "")
    monkeypatch.setattr(handler, "ask_bedrock", lambda p: pytest.fail("Bedrock ne doit pas être appelé"))
    out = handler.lambda_handler(EVENT)
    assert out["suspected_comp"] == "comp2"
    item = aws["ddb"].Table("tickets").scan()["Items"][0]
    assert "45 jours" in item["recommendation"] and "error2" in item["recommendation"]
    assert len(_mails(aws)) == 1


def test_parse_answer():
    assert handler.parse_answer('```json\n{"suspected_comp": "COMP3", "recommendation": "ok"}\n```') == ("comp3", "ok")
    assert handler.parse_answer('{"suspected_comp": "moteur", "recommendation": "ok"}')[0] == "inconnu"
    with pytest.raises(ValueError):
        handler.parse_answer("pas de json")


def test_prompt_contains_facts():
    p = handler.build_prompt(EVENT, [{"datetime": "d", "error": "error2"}], {"comp1": None}, {"model": "model2", "age": 8})
    assert "0.9993" in p and "error2" in p and "model2" in p


def test_missing_field(aws):
    with pytest.raises(ValueError):
        handler.lambda_handler({"machineID": 3})


def _llm_env(monkeypatch):
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.setenv("LLM_BASE_URL", "https://api.example.com/v1")
    monkeypatch.setenv("LLM_MODEL", "m")


def test_bedrock_fails_then_llm_api_answers(aws, monkeypatch):
    _llm_env(monkeypatch)
    monkeypatch.setattr(handler, "ask_bedrock", lambda p: (_ for _ in ()).throw(RuntimeError("denied")))
    monkeypatch.setattr(handler, "ask_llm_api", lambda p: '{"suspected_comp": "comp4", "recommendation": "Via API."}')
    out = handler.lambda_handler(EVENT)
    assert out["suspected_comp"] == "comp4"
    assert aws["ddb"].Table("tickets").scan()["Items"][0]["recommendation"] == "Via API."


def test_all_llm_fail_then_rules(aws, monkeypatch):
    _llm_env(monkeypatch)
    boom = lambda p: (_ for _ in ()).throw(RuntimeError("down"))
    monkeypatch.setattr(handler, "ask_bedrock", boom)
    monkeypatch.setattr(handler, "ask_llm_api", boom)
    out = handler.lambda_handler(EVENT)
    assert out["suspected_comp"] == "comp2"
    assert "45 jours" in aws["ddb"].Table("tickets").scan()["Items"][0]["recommendation"]


def test_llm_api_request_format(monkeypatch):
    _llm_env(monkeypatch)
    seen = {}

    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return json.dumps({"choices": [{"message": {"content": "salut"}}]}).encode()

    def fake_urlopen(req, timeout):
        seen["url"], seen["auth"], seen["timeout"] = req.full_url, req.get_header("Authorization"), timeout
        seen["body"] = json.loads(req.data)
        return Resp()

    monkeypatch.setattr(handler.urllib.request, "urlopen", fake_urlopen)
    assert handler.ask_llm_api("question") == "salut"
    assert seen["url"] == "https://api.example.com/v1/chat/completions"
    assert seen["auth"] == "Bearer k" and seen["timeout"] == 10
    assert seen["body"]["messages"][0]["content"] == "question" and seen["body"]["model"] == "m"
    assert seen["body"]["max_tokens"] == 1500 and "reasoning_effort" not in seen["body"]
    monkeypatch.setenv("LLM_REASONING_EFFORT", "low")
    monkeypatch.setenv("LLM_MAX_TOKENS", "800")
    handler.ask_llm_api("q")
    assert seen["body"]["reasoning_effort"] == "low" and seen["body"]["max_tokens"] == 800