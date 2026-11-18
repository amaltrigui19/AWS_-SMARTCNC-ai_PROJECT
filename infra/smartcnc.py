"""Outil de déploiement SmartCNC (partie A, semaine 1) — boto3, sans AWS CLI.

Identifiants : copier ceux du Learner Lab (AWS Details > AWS CLI) dans ~/.aws/credentials.
Tous les services utilisent le rôle LabRole (le Lab interdit de créer des rôles IAM).

Commandes (python infra/smartcnc.py <commande>) :
    bucket            crée le bucket et les préfixes raw/ replay/ stream/ models/
    upload-raw        envoie data/PdM_*.csv dans raw/
    upload-replay     envoie data/replay/<nom>/ dans replay/<nom>/   (--name test|full)
    tables            crée replay_state, machine_state, tickets (à la demande)
    stream-up         crée Kinesis (1 shard) + Firehose vers stream/events/
    stream-down       arrête le simulateur, supprime Firehose puis Kinesis
    deploy-simulator  crée ou met à jour la Lambda + règle EventBridge (désactivée)
    sim-start         active la règle (1 lot par minute)
    sim-stop          désactive la règle
    sim-reset         efface la position (le prochain lot repart du début du manifest)
    sim-once          invoque la Lambda une fois, tout de suite
    status            état du flux, de la règle, de la position et de stream/
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
STREAM = "smartcnc-measurements"
FIREHOSE = "smartcnc-archive"
SIM_LAMBDA = "smartcnc-simulator"
SIM_RULE = "smartcnc-simulator-every-minute"
PREFIXES = ["raw/", "replay/", "stream/", "models/"]
RAW_FILES = ["PdM_telemetry.csv", "PdM_errors.csv", "PdM_maint.csv", "PdM_failures.csv", "PdM_machines.csv"]

session = boto3.session.Session(region_name=REGION)


def account_id() -> str:
    return session.client("sts").get_caller_identity()["Account"]


def lab_role_arn() -> str:
    return os.environ.get("LAB_ROLE_ARN", f"arn:aws:iam::{account_id()}:role/LabRole")


def bucket_name() -> str:
    return os.environ.get("SMARTCNC_BUCKET", f"smartcnc-{account_id()}")


# ------------------------------------------------------------------------- S3

def cmd_bucket(args):
    s3, name = session.client("s3"), bucket_name()
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": REGION})
        print(f"Bucket créé : {name}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
        print(f"Bucket déjà présent : {name}")
    for p in PREFIXES:
        s3.put_object(Bucket=name, Key=p)
    print("Préfixes :", " ".join(PREFIXES))


def cmd_upload_raw(args):
    s3, name = session.client("s3"), bucket_name()
    data = Path(args.data)
    missing = [f for f in RAW_FILES if not (data / f).exists()]
    if missing:
        sys.exit(f"Fichiers manquants dans {data} : {', '.join(missing)}")
    for f in RAW_FILES:
        s3.upload_file(str(data / f), name, f"raw/{f}")
        print(f"raw/{f}")


def cmd_upload_replay(args):
    s3, name = session.client("s3"), bucket_name()
    src = Path(args.data) / "replay" / args.name
    if not (src / "manifest.json").exists():
        sys.exit(f"{src}/manifest.json introuvable : lancer d'abord src/simulator/prepare_replay.py")
    prefix = f"replay/{args.name}/"
    old = s3.list_objects_v2(Bucket=name, Prefix=prefix).get("Contents", [])
    if old:
        s3.delete_objects(Bucket=name, Delete={"Objects": [{"Key": o["Key"]} for o in old]})
    files = sorted(src.glob("*.jsonl")) + [src / "manifest.json"]
    for f in files:
        s3.upload_file(str(f), name, prefix + f.name)
    print(f"{len(files)} fichiers envoyés dans s3://{name}/{prefix}")


# ------------------------------------------------------------------- DynamoDB

TABLES = {
    "replay_state": ("id", "S"),
    "machine_state": ("machineID", "N"),
    "tickets": ("ticketID", "S"),
}


def cmd_tables(args):
    ddb = session.client("dynamodb")
    for table, (key, typ) in TABLES.items():
        try:
            ddb.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": key, "AttributeType": typ}],
                KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            print(f"Table créée : {table}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceInUseException":
                raise
            print(f"Table déjà présente : {table}")
    for table in TABLES:
        ddb.get_waiter("table_exists").wait(TableName=table)


# -------------------------------------------------------------------- Kinesis

def cmd_stream_up(args):
    kin, fh = session.client("kinesis"), session.client("firehose")
    try:
        kin.create_stream(StreamName=STREAM, ShardCount=1, StreamModeDetails={"StreamMode": "PROVISIONED"})
        print(f"Kinesis en création : {STREAM}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    kin.get_waiter("stream_exists").wait(StreamName=STREAM)
    stream_arn = kin.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]["StreamARN"]
    print(f"Kinesis actif : {stream_arn}")

    role = lab_role_arn()
    try:
        fh.create_delivery_stream(
            DeliveryStreamName=FIREHOSE,
            DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={"KinesisStreamARN": stream_arn, "RoleARN": role},
            ExtendedS3DestinationConfiguration={
                "RoleARN": role,
                "BucketARN": f"arn:aws:s3:::{bucket_name()}",
                # Firehose ajoute AAAA/MM/JJ/HH/ (heure d'arrivée, pas heure simulée).
                "Prefix": "stream/events/",
                "ErrorOutputPrefix": "stream-errors/!{firehose:error-output-type}/",
                "BufferingHints": {"IntervalInSeconds": 60, "SizeInMBs": 5},
                "CompressionFormat": "UNCOMPRESSED",
            },
        )
        print(f"Firehose en création : {FIREHOSE}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    for _ in range(60):
        status = fh.describe_delivery_stream(DeliveryStreamName=FIREHOSE)["DeliveryStreamDescription"]["DeliveryStreamStatus"]
        if status == "ACTIVE":
            break
        time.sleep(5)
    print(f"Firehose : {status}")


def cmd_stream_down(args):
    try:
        cmd_sim_stop(args)
    except ClientError:
        pass
    kin, fh = session.client("kinesis"), session.client("firehose")
    try:
        fh.delete_delivery_stream(DeliveryStreamName=FIREHOSE)
        print(f"Firehose supprimé : {FIREHOSE}")
        while True:  # Kinesis ne peut pas être supprimé tant que Firehose le lit
            fh.describe_delivery_stream(DeliveryStreamName=FIREHOSE)
            time.sleep(5)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    try:
        kin.delete_stream(StreamName=STREAM, EnforceConsumerDeletion=True)
        kin.get_waiter("stream_not_exists").wait(StreamName=STREAM)
        print(f"Kinesis supprimé : {STREAM}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        print("Kinesis déjà supprimé")


# ----------------------------------------------------------------- Simulateur

def _zip_simulator() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(ROOT / "src" / "simulator" / "handler.py", "handler.py")
    return buf.getvalue()


def cmd_deploy_simulator(args):
    lam, events = session.client("lambda"), session.client("events")
    env = {"Variables": {
        "BUCKET": bucket_name(),
        "REPLAY_PREFIX": f"replay/{args.replay}/",
        "STREAM_NAME": STREAM,
        "STATE_TABLE": "replay_state",
        "HOURS_PER_BATCH": str(args.hours),
    }}
    code = _zip_simulator()
    try:
        lam.create_function(
            FunctionName=SIM_LAMBDA, Runtime="python3.12", Role=lab_role_arn(),
            Handler="handler.lambda_handler", Code={"ZipFile": code},
            Timeout=120, MemorySize=256, Environment=env,
        )
        print(f"Lambda créée : {SIM_LAMBDA}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=SIM_LAMBDA, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=SIM_LAMBDA)
        lam.update_function_configuration(FunctionName=SIM_LAMBDA, Environment=env, Timeout=120)
        print(f"Lambda mise à jour : {SIM_LAMBDA}")
    lam.get_waiter("function_active_v2").wait(FunctionName=SIM_LAMBDA)
    # Un seul appel à la fois : deux lots en parallèle casseraient l'ordre.
    lam.put_function_concurrency(FunctionName=SIM_LAMBDA, ReservedConcurrentExecutions=1)
    # Un lot raté n'est pas rejoué automatiquement : la position n'a pas avancé, le suivant le reprend.
    lam.put_function_event_invoke_config(FunctionName=SIM_LAMBDA, MaximumRetryAttempts=0)

    fn_arn = lam.get_function(FunctionName=SIM_LAMBDA)["Configuration"]["FunctionArn"]
    rule_arn = events.put_rule(Name=SIM_RULE, ScheduleExpression="rate(1 minute)", State="DISABLED")["RuleArn"]
    events.put_targets(Rule=SIM_RULE, Targets=[{"Id": "simulator", "Arn": fn_arn}])
    try:
        lam.add_permission(FunctionName=SIM_LAMBDA, StatementId="eventbridge-invoke",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=rule_arn)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    print(f"Règle {SIM_RULE} prête (désactivée). Rejeu : replay/{args.replay}/, {args.hours} h par lot")


def cmd_sim_start(args):
    session.client("events").enable_rule(Name=SIM_RULE)
    print("Simulateur démarré (1 lot par minute)")


def cmd_sim_stop(args):
    session.client("events").disable_rule(Name=SIM_RULE)
    print("Simulateur arrêté")


def cmd_sim_reset(args):
    session.client("dynamodb").delete_item(TableName="replay_state", Key={"id": {"S": "simulator"}})
    print("Position effacée : le prochain lot repart du début du manifest")


def cmd_sim_once(args):
    resp = session.client("lambda").invoke(FunctionName=SIM_LAMBDA, LogType="Tail")
    print(resp["Payload"].read().decode())
    if resp.get("FunctionError"):
        import base64
        print(base64.b64decode(resp["LogResult"]).decode())


def cmd_status(args):
    def safe(fn, default="absent"):
        try:
            return fn()
        except ClientError as e:
            return f"{default} ({e.response['Error']['Code']})"

    print("Bucket   :", bucket_name())
    print("Kinesis  :", safe(lambda: session.client("kinesis").describe_stream_summary(StreamName=STREAM)
                              ["StreamDescriptionSummary"]["StreamStatus"]))
    print("Firehose :", safe(lambda: session.client("firehose").describe_delivery_stream(DeliveryStreamName=FIREHOSE)
                              ["DeliveryStreamDescription"]["DeliveryStreamStatus"]))
    print("Règle    :", safe(lambda: session.client("events").describe_rule(Name=SIM_RULE)["State"]))
    print("Lambda   :", safe(lambda: session.client("lambda").get_function_configuration(FunctionName=SIM_LAMBDA)
                              ["Environment"]["Variables"]["REPLAY_PREFIX"]))
    print("Position :", safe(lambda: session.client("dynamodb").get_item(
        TableName="replay_state", Key={"id": {"S": "simulator"}}).get("Item", {}).get("position", {}).get("S", "début")))

    def count(prefix):
        def run():
            pages = session.client("s3").get_paginator("list_objects_v2").paginate(
                Bucket=bucket_name(), Prefix=prefix)
            objs = [o for p in pages for o in p.get("Contents", [])]
            return f"{len(objs)} fichiers, {sum(o['Size'] for o in objs) / 1e6:.1f} Mo"
        return run
    print("stream/  :", safe(count("stream/events/")))
    print("erreurs  :", safe(count("stream-errors/")))

    def firehose_metrics():
        from datetime import datetime, timedelta, timezone
        cw, now = session.client("cloudwatch"), datetime.now(timezone.utc)
        out = []
        for metric in ["DataReadFromKinesisStream.Records", "DeliveryToS3.Records", "DeliveryToS3.Success"]:
            pts = cw.get_metric_statistics(
                Namespace="AWS/Firehose", MetricName=metric,
                Dimensions=[{"Name": "DeliveryStreamName", "Value": FIREHOSE}],
                StartTime=now - timedelta(minutes=30), EndTime=now, Period=1800,
                Statistics=["Sum" if metric.endswith("Records") else "Average"])["Datapoints"]
            val = pts[0].get("Sum", pts[0].get("Average")) if pts else "aucune donnée"
            out.append(f"{metric} = {val}")
        return " | ".join(out)
    print("Firehose (30 min) :", safe(firehose_metrics))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("bucket")
    sub.add_parser("upload-raw").add_argument("--data", default=str(ROOT / "data"))
    up = sub.add_parser("upload-replay")
    up.add_argument("--name", default="test")
    up.add_argument("--data", default=str(ROOT / "data"))
    sub.add_parser("tables")
    sub.add_parser("stream-up")
    sub.add_parser("stream-down")
    dep = sub.add_parser("deploy-simulator")
    dep.add_argument("--replay", default="test", help="sous-dossier de replay/ (test ou full)")
    dep.add_argument("--hours", type=int, default=60, help="heures simulées par lot")
    for c in ["sim-start", "sim-stop", "sim-reset", "sim-once", "status"]:
        sub.add_parser(c)
    args = p.parse_args()
    globals()["cmd_" + args.cmd.replace("-", "_")](args)


if __name__ == "__main__":
    main()
