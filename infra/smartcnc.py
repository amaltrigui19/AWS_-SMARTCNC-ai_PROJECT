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

Semaine 2 (scoring) :
    upload-model      envoie models/model.pkl, columns.json, threshold.json dans s3://…/models/
    load-machines     écrit l'état initial (préchauffé) des machines du rejeu dans machine_state
    push-scoring      construit l'image Docker de la Lambda de scoring et la pousse dans ECR
    deploy-scoring    crée ou met à jour la Lambda de scoring (+ son déclencheur Kinesis si le flux existe)
    risks             affiche le dernier risque de chaque machine (machine_state)
    logs              derniers journaux CloudWatch (--name scoring|simulator, --minutes 30)

Semaine 3 (historique et tableau de bord) :
    athena-setup      crée la base smartcnc : tables events, risks, tickets, failures + vues d'analyse
    athena-report     alertes du dernier rejeu comparées aux pannes réelles (délai d'anticipation)
    athena-check      diagnostic si athena-report est vide (Lambda, S3, tables)
    dashboard         crée ou met à jour le tableau de bord CloudWatch « SmartCNC »

Semaine 4 (agent, fiabilité, coûts) :
    alerts-topic      crée le sujet SNS smartcnc-alerts et abonne des e-mails (--email a@b --email c@d)
    deploy-agent      déploie la Lambda agent de B (src/agent/, clé d'API lue dans .env)
    agent-test        appelle l'agent une fois (--machine 3) pour vérifier ticket + e-mail
    reliability       DLQ (SQS), nouvelles tentatives limitées, 6 alarmes CloudWatch -> e-mail
    alarms            état des alarmes
    cleanup           supprime anciennes images ECR, résultats Athena, erreurs Firehose (--old-runs : anciens rejeux)
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
SCORING_LAMBDA = "smartcnc-scoring"
SCORING_REPO = "smartcnc-scoring"
MODEL_FILES = ["model.pkl", "columns.json", "threshold.json"]
AGENT_LAMBDA = "smartcnc-agent"          # Lambda de B (contrat 4)
ATHENA_DB = "smartcnc"
DASHBOARD = "SmartCNC"
TOPIC = "smartcnc-alerts"                # e-mails d'alerte (agent) et alarmes CloudWatch
SCORING_DLQ = "smartcnc-scoring-dlq"     # lots Kinesis abandonnés après 2 tentatives
AGENT_DLQ = "smartcnc-agent-dlq"         # alertes que l'agent n'a pas pu traiter
AGENT_ENV_KEYS = ["LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL", "LLM_REASONING_EFFORT", "MODEL_ID"]
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
    _ensure_scoring_trigger(stream_arn)


def cmd_stream_down(args):
    try:
        cmd_sim_stop(args)
    except ClientError:
        pass
    _delete_scoring_trigger()
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
    # On arrête d'abord le simulateur : sinon un lot peut partir avant 'load-machines'.
    session.client("events").disable_rule(Name=SIM_RULE)
    print("Simulateur arrêté (relancer 'sim-start' après 'load-machines')")
    session.client("dynamodb").delete_item(TableName="replay_state", Key={"id": {"S": "simulator"}})
    print("Position effacée : le prochain lot repart du début du manifest")
    print("⚠ Relancer aussi 'load-machines' : sinon la Lambda de scoring ignore les événements "
          "plus anciens que ceux déjà traités.")


# ------------------------------------------------------------------- Scoring

def cmd_upload_model(args):
    s3, name, src = session.client("s3"), bucket_name(), Path(args.models)
    missing = [f for f in MODEL_FILES if not (src / f).exists()]
    if missing:
        sys.exit(f"Fichiers manquants dans {src} : {', '.join(missing)}")
    for f in MODEL_FILES:
        s3.upload_file(str(src / f), name, f"models/{f}")
        print(f"models/{f}")
    print("Si la Lambda de scoring tourne déjà, relancer 'deploy-scoring' pour qu'elle recharge le modèle.")


def cmd_load_machines(args):
    sys.path.insert(0, str(ROOT / "src" / "scoring"))
    from init_state import initial_states
    from state_store import batch_put_states

    manifest_path = Path(args.data) / "replay" / args.replay / "manifest.json"
    if not manifest_path.exists():
        sys.exit(f"{manifest_path} introuvable : lancer d'abord prepare_replay.py")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    machines = None if manifest["machines"] == "all" else manifest["machines"]
    print(f"Préchauffage des états au {manifest['start']} (lecture des CSV, ~30 s)…")
    states = initial_states(Path(args.data), manifest["start"], machines)
    # run_id identifie ce rejeu dans les archives (Athena compare les alertes d'un même rejeu aux pannes).
    run_id = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    for s in states:
        s["run_id"] = run_id
    batch_put_states(session.client("dynamodb"), "machine_state", states)
    print(f"{len(states)} machines écrites dans machine_state (rejeu '{args.replay}', run_id {run_id})")


def _ecr_login(ecr) -> str:
    import base64
    import subprocess
    auth = ecr.get_authorization_token()["authorizationData"][0]
    user, password = base64.b64decode(auth["authorizationToken"]).decode().split(":", 1)
    registry = auth["proxyEndpoint"].replace("https://", "")
    subprocess.run(["docker", "login", "--username", user, "--password-stdin", registry],
                   input=password.encode(), check=True)
    return registry


def cmd_push_scoring(args):
    import subprocess
    ecr = session.client("ecr")
    try:
        ecr.create_repository(repositoryName=SCORING_REPO)
        print(f"Dépôt ECR créé : {SCORING_REPO}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "RepositoryAlreadyExistsException":
            raise
    registry = _ecr_login(ecr)
    uri = f"{registry}/{SCORING_REPO}:latest"
    # --provenance=false : sinon Docker produit un index d'images que Lambda refuse.
    subprocess.run(["docker", "build", "--platform", "linux/amd64", "--provenance=false",
                    "-f", "src/scoring/Dockerfile", "-t", uri, "."], cwd=ROOT, check=True)
    subprocess.run(["docker", "push", uri], check=True)
    print(f"Image poussée : {uri}")


def _scoring_image_uri() -> str:
    repo = session.client("ecr").describe_repositories(repositoryNames=[SCORING_REPO])["repositories"][0]
    return repo["repositoryUri"] + ":latest"


def cmd_deploy_scoring(args):
    lam = session.client("lambda")
    image = _scoring_image_uri()
    # L'agent (B) n'est appelé que s'il est déjà déployé ; sinon les alertes sont seulement journalisées.
    try:
        lam.get_function(FunctionName=AGENT_LAMBDA)
        agent = AGENT_LAMBDA
        print(f"Agent trouvé : {AGENT_LAMBDA} (appelé à chaque alerte)")
    except ClientError:
        agent = ""
        print(f"Agent {AGENT_LAMBDA} absent : alertes journalisées seulement (relancer deploy-scoring après B)")
    env = {"Variables": {"BUCKET": bucket_name(), "MODEL_PREFIX": "models/", "STATE_TABLE": "machine_state",
                         "ARCHIVE_PREFIX": "stream/risks/", "AGENT_FUNCTION": agent}}
    try:
        lam.create_function(
            FunctionName=SCORING_LAMBDA, PackageType="Image", Code={"ImageUri": image},
            Role=lab_role_arn(), Timeout=120, MemorySize=1024, Environment=env,
        )
        print(f"Lambda créée : {SCORING_LAMBDA}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=SCORING_LAMBDA, ImageUri=image)
        lam.get_waiter("function_updated_v2").wait(FunctionName=SCORING_LAMBDA)
        lam.update_function_configuration(FunctionName=SCORING_LAMBDA, Environment=env,
                                          Timeout=120, MemorySize=1024)
        print(f"Lambda mise à jour : {SCORING_LAMBDA} (nouveau conteneur : le modèle sera rechargé)")
    lam.get_waiter("function_active_v2").wait(FunctionName=SCORING_LAMBDA)
    try:
        arn = session.client("kinesis").describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]["StreamARN"]
        _ensure_scoring_trigger(arn)
    except ClientError:
        print("Kinesis absent : le déclencheur sera créé par 'stream-up'")


def _ensure_scoring_trigger(stream_arn: str) -> None:
    """Relie Kinesis à la Lambda de scoring (si elle existe)."""
    lam = session.client("lambda")
    try:
        lam.get_function(FunctionName=SCORING_LAMBDA)
    except ClientError:
        return  # pas encore déployée (semaine 1)
    existing = lam.list_event_source_mappings(FunctionName=SCORING_LAMBDA, EventSourceArn=stream_arn)
    if existing["EventSourceMappings"]:
        print("Déclencheur Kinesis -> scoring : déjà présent")
        return
    extra = {}
    dlq = _queue_arn(SCORING_DLQ)   # semaine 4 : si la DLQ existe, les lots abandonnés y sont décrits
    if dlq:
        extra["DestinationConfig"] = {"OnFailure": {"Destination": dlq}}
    lam.create_event_source_mapping(
        FunctionName=SCORING_LAMBDA, EventSourceArn=stream_arn,
        StartingPosition="TRIM_HORIZON",       # rien n'est perdu si le simulateur a démarré avant
        BatchSize=1000, MaximumBatchingWindowInSeconds=5,
        MaximumRetryAttempts=2, BisectBatchOnFunctionError=True, **extra,
    )
    print("Déclencheur Kinesis -> scoring : créé" + (" (avec DLQ)" if dlq else ""))


def _delete_scoring_trigger() -> None:
    lam = session.client("lambda")
    try:
        mappings = lam.list_event_source_mappings(FunctionName=SCORING_LAMBDA)["EventSourceMappings"]
    except ClientError:
        return
    for m in mappings:
        lam.delete_event_source_mapping(UUID=m["UUID"])
        print("Déclencheur Kinesis -> scoring : supprimé")


def cmd_risks(args):
    sys.path.insert(0, str(ROOT / "src" / "scoring"))
    from state_store import from_item
    pages = session.client("dynamodb").get_paginator("scan").paginate(TableName="machine_state")
    states = sorted((from_item(i) for p in pages for i in p["Items"]), key=lambda s: s["machineID"])
    print(f"{'machine':>7}  {'dernier événement':<19}  {'dernier risque à':<19}  risque")
    for s in states:
        risk = s.get("risk")
        print(f"{s['machineID']:>7}  {str(s.get('last_datetime') or '-'):<19}  "
              f"{str(s.get('risk_datetime') or '-'):<19}  {'-' if risk is None else f'{risk:.3f}'}")
    print(f"{len(states)} machines")


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

    def scoring_trigger():
        lam = session.client("lambda")
        lam.get_function(FunctionName=SCORING_LAMBDA)
        maps = lam.list_event_source_mappings(FunctionName=SCORING_LAMBDA)["EventSourceMappings"]
        if not maps:
            return "Lambda déployée, mais AUCUN déclencheur Kinesis (lancer stream-up ou deploy-scoring)"
        return " | ".join(f"{m['State']}, dernier traitement : {m.get('LastProcessingResult', '?')}" for m in maps)
    print("Scoring  :", safe(scoring_trigger))


def cmd_logs(args):
    """Derniers messages CloudWatch d'une Lambda (erreurs comprises)."""
    fn = {"scoring": SCORING_LAMBDA, "simulator": SIM_LAMBDA, "agent": AGENT_LAMBDA}[args.name]
    logs = session.client("logs")
    start = int((time.time() - args.minutes * 60) * 1000)
    try:
        pages = logs.get_paginator("filter_log_events").paginate(
            logGroupName=f"/aws/lambda/{fn}", startTime=start)
        events = [e for p in pages for e in p["events"]]
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            sys.exit(f"Aucun journal pour {fn} : la Lambda n'a jamais été appelée.")
        raise
    if not events:
        print(f"Aucun message de {fn} dans les {args.minutes} dernières minutes : elle n'a pas été appelée.")
        return
    for e in events[-args.lines:]:
        stamp = time.strftime("%H:%M:%S", time.localtime(e["timestamp"] / 1000))
        print(stamp, e["message"].rstrip())
    errors = [e for e in events if any(w in e["message"] for w in ("Error", "Traceback", "Task timed out", "ERROR"))]
    print(f"\n{len(events)} messages, dont {len(errors)} contenant une erreur ({args.minutes} min).")


# -------------------------------------------------------------------- Athena

DT = "'%Y-%m-%d %H:%i:%s'"   # format des dates (contrat 1) pour date_parse / date_format


def athena_ddl(bucket: str) -> list[str]:
    json_serde = ("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' "
                  "WITH SERDEPROPERTIES ('ignore.malformed.json'='true')")
    db = ATHENA_DB
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        # Événements rejoués (Firehose).
        f"""CREATE EXTERNAL TABLE IF NOT EXISTS {db}.events (
              type string, machineid int, `datetime` string, volt double, rotate double,
              pressure double, vibration double, errorid string, comp string)
            {json_serde} LOCATION 's3://{bucket}/stream/events/'""",
        # Risques archivés par la Lambda de scoring (contrat 4).
        f"""CREATE EXTERNAL TABLE IF NOT EXISTS {db}.risks (
              run_id string, machineid int, `datetime` string, risk double,
              threshold double, above boolean, alert boolean)
            {json_serde} LOCATION 's3://{bucket}/stream/risks/'""",
        # Tickets archivés par l'agent de B (contrats 3 et 4).
        f"""CREATE EXTERNAL TABLE IF NOT EXISTS {db}.tickets (
              ticketid string, machineid int, created_at string, risk double, suspected_comp string,
              recommendation string, status string, run_id string)
            {json_serde} LOCATION 's3://{bucket}/stream/tickets/'""",
        # Vérité terrain : jamais rejouée, seulement lue ici (copie de raw/PdM_failures.csv).
        f"""CREATE EXTERNAL TABLE IF NOT EXISTS {db}.failures (
              `datetime` string, machineid string, failure string)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION 's3://{bucket}/reference/failures/'
            TBLPROPERTIES ('skip.header.line.count'='1')""",
        # Risques du dernier rejeu seulement.
        f"""CREATE OR REPLACE VIEW {db}.risks_last_run AS
            SELECT * FROM {db}.risks
            WHERE run_id = (SELECT max(run_id) FROM {db}.risks WHERE run_id <> 'sans-run')""",
        # Pour chaque panne réelle de la période rejouée : premier risque élevé et première alerte
        # dans les 24 h qui précèdent, et l'avance en heures.
        f"""CREATE OR REPLACE VIEW {db}.failure_lead_time AS
            WITH r AS (SELECT * FROM {db}.risks_last_run),
            f AS (
              SELECT DISTINCT CAST(machineid AS int) AS machineid, "datetime" AS panne, failure
              FROM {db}.failures
              WHERE "datetime" >= (SELECT min("datetime") FROM r)
                AND "datetime" <= (SELECT max("datetime") FROM r)
                AND CAST(machineid AS int) IN (SELECT DISTINCT machineid FROM r))
            SELECT f.machineid, f.panne, f.failure,
                   min(CASE WHEN r.above THEN r."datetime" END) AS premier_risque_eleve,
                   min(CASE WHEN r.alert THEN r."datetime" END) AS premiere_alerte,
                   date_diff('hour', date_parse(min(CASE WHEN r.above THEN r."datetime" END), {DT}),
                             date_parse(f.panne, {DT})) AS avance_h
            FROM f LEFT JOIN r
              ON r.machineid = f.machineid
             AND r."datetime" < f.panne
             AND r."datetime" >= date_format(date_parse(f.panne, {DT}) - INTERVAL '24' HOUR, {DT})
            GROUP BY f.machineid, f.panne, f.failure""",
        # Pour chaque alerte du dernier rejeu : y a-t-il eu une vraie panne dans les 24 h suivantes ?
        f"""CREATE OR REPLACE VIEW {db}.alert_quality AS
            SELECT r.machineid, r."datetime" AS alerte, r.risk,
                   min(f."datetime") AS panne_suivante, min(f."datetime") IS NOT NULL AS juste
            FROM {db}.risks_last_run r LEFT JOIN {db}.failures f
              ON CAST(f.machineid AS int) = r.machineid
             AND f."datetime" > r."datetime"
             AND f."datetime" <= date_format(date_parse(r."datetime", {DT}) + INTERVAL '24' HOUR, {DT})
            WHERE r.alert
            GROUP BY r.machineid, r."datetime", r.risk""",
    ]


def athena_run(sql: str, fetch: bool = False):
    ath = session.client("athena")
    qid = ath.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": f"s3://{bucket_name()}/athena-results/"},
    )["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(f"Requête Athena {st['State']} : {st.get('StateChangeReason')}\n{sql}")
    if not fetch:
        return None
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue", "") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    return rows   # première ligne = noms des colonnes


def _print_table(rows) -> None:
    if not rows:
        print("(aucun résultat)")
        return
    widths = [max(len(str(r[i])) for r in rows) for i in range(len(rows[0]))]
    for n, r in enumerate(rows):
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
        if n == 0:
            print("  ".join("-" * w for w in widths))


def cmd_athena_setup(args):
    s3, name = session.client("s3"), bucket_name()
    s3.copy_object(Bucket=name, Key="reference/failures/PdM_failures.csv",
                   CopySource={"Bucket": name, "Key": "raw/PdM_failures.csv"})
    print("Pannes copiées dans reference/failures/ (vérité terrain pour Athena)")
    for sql in athena_ddl(name):
        athena_run(sql)
        print("OK :", " ".join(sql.split())[:70], "…")
    print(f"Base Athena '{ATHENA_DB}' prête (console Athena > éditeur de requêtes).")


def cmd_athena_report(args):
    db = ATHENA_DB
    print("== Pannes réelles du dernier rejeu : ont-elles été annoncées ? ==")
    _print_table(athena_run(f"SELECT * FROM {db}.failure_lead_time ORDER BY panne", fetch=True))
    print("\n== Résumé ==")
    _print_table(athena_run(f"""
        SELECT count(*) AS pannes,
               count(premier_risque_eleve) AS annoncees_24h_avant,
               approx_percentile(avance_h, 0.5) AS avance_mediane_h,
               (SELECT count(*) FROM {db}.alert_quality) AS alertes,
               (SELECT count_if(juste) FROM {db}.alert_quality) AS alertes_justes
        FROM {db}.failure_lead_time""", fetch=True))


def cmd_athena_check(args):
    """Diagnostic : où s'arrêtent les données entre la Lambda de scoring et Athena ?"""
    db, name = ATHENA_DB, bucket_name()
    env = session.client("lambda").get_function_configuration(FunctionName=SCORING_LAMBDA)["Environment"]["Variables"]
    print("1. Lambda de scoring : ARCHIVE_PREFIX =", repr(env.get("ARCHIVE_PREFIX")),
          "(absent = ancienne version : relancer push-scoring puis deploy-scoring)")
    try:
        deployed = session.client("lambda").get_function(FunctionName=SCORING_LAMBDA)["Code"].get("ResolvedImageUri", "")
        latest = session.client("ecr").describe_images(
            repositoryName=SCORING_REPO, imageIds=[{"imageTag": "latest"}])["imageDetails"][0]
        same = deployed.endswith(latest["imageDigest"])
        print(f"   Image : poussée le {latest['imagePushedAt']:%Y-%m-%d %H:%M} ;",
              "la Lambda l'utilise ✓" if same else
              "la Lambda utilise une AUTRE image ✗ (relancer deploy-scoring)")
    except ClientError as e:
        print("   Image : vérification impossible", e.response["Error"]["Code"])
    pages = session.client("s3").get_paginator("list_objects_v2").paginate(Bucket=name, Prefix="stream/risks/")
    keys = [o["Key"] for p in pages for o in p.get("Contents", [])]
    runs = sorted({k.split("/")[2] for k in keys if k.count("/") >= 3})
    print(f"2. S3 stream/risks/ : {len(keys)} fichiers, rejeux : {', '.join(runs) or 'aucun'}")
    print("3. Athena, table risks :")
    _print_table(athena_run(f"""SELECT run_id, count(*) AS lignes, min("datetime") AS debut, max("datetime") AS fin,
                                       count_if(above) AS risques_eleves, count_if(alert) AS alertes
                                FROM {db}.risks GROUP BY run_id ORDER BY run_id""", fetch=True))
    print("4. Athena, table failures (vérité terrain) :")
    _print_table(athena_run(f"""SELECT count(*) AS pannes, min("datetime") AS premiere, max("datetime") AS derniere
                                FROM {db}.failures""", fetch=True))


# ----------------------------------------------------------- Tableau de bord

def dashboard_body() -> dict:
    def metric(title, metrics, stat="Sum", x=0, y=0, w=12, h=6, view="timeSeries"):
        return {"type": "metric", "x": x, "y": y, "width": w, "height": h, "properties": {
            "title": title, "region": REGION, "view": view, "stat": stat, "period": 60, "metrics": metrics}}

    def logs(title, query, x=0, y=0, w=12, h=8):
        return {"type": "log", "x": x, "y": y, "width": w, "height": h, "properties": {
            "title": title, "region": REGION, "view": "table",
            "query": f"SOURCE '/aws/lambda/{SCORING_LAMBDA}' | {query}"}}

    return {"widgets": [
        {"type": "text", "x": 0, "y": 0, "width": 24, "height": 2, "properties": {"markdown":
            "# SmartCNC AI — maintenance prédictive\nRejeu Azure PdM · risque de panne dans les 24 h · "
            "alerte si risque ≥ seuil (au plus une par machine toutes les 12 h simulées)"}},
        metric("Alertes et risques élevés (par minute)",
               [["SmartCNC", "Alerts"], ["SmartCNC", "AboveThreshold"]], x=0, y=2),
        metric("Risque maximal du lot", [["SmartCNC", "MaxRisk"]], stat="Maximum", x=12, y=2),
        logs("Dernier risque de chaque machine",
             "filter kind = 'machine_risk' | stats latest(risk) as risque, latest(`datetime`) as heure_simulee "
             "by machineID | sort risque desc | limit 100", x=0, y=8),
        logs("Dernières alertes",
             "filter kind = 'alert' | fields `datetime` as heure_simulee, machineID, risk, agent "
             "| sort @timestamp desc | limit 20", x=12, y=8),
        metric("Événements traités / risques calculés",
               [["SmartCNC", "EventsProcessed"], ["SmartCNC", "RisksComputed"]], x=0, y=16, w=8),
        metric("Retard de lecture Kinesis (ms)",
               [["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", STREAM]],
               stat="Maximum", x=8, y=16, w=8),
        metric("Erreurs des Lambdas",
               [["AWS/Lambda", "Errors", "FunctionName", SCORING_LAMBDA],
                ["AWS/Lambda", "Errors", "FunctionName", SIM_LAMBDA],
                ["AWS/Lambda", "Errors", "FunctionName", AGENT_LAMBDA]], x=16, y=16, w=8),
    ]}


def cmd_dashboard(args):
    session.client("cloudwatch").put_dashboard(DashboardName=DASHBOARD,
                                               DashboardBody=json.dumps(dashboard_body()))
    print(f"Tableau de bord prêt : https://{REGION}.console.aws.amazon.com/cloudwatch/home"
          f"?region={REGION}#dashboards/dashboard/{DASHBOARD}")


# ------------------------------------------------- Semaine 4 : agent et SNS

def _queue_arn(name: str, create: bool = False) -> str | None:
    sqs = session.client("sqs")
    try:
        url = sqs.create_queue(QueueName=name, Attributes={"MessageRetentionPeriod": "1209600"})["QueueUrl"] \
            if create else sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError:
        return None
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def _topic_arn() -> str:
    return session.client("sns").create_topic(Name=TOPIC)["TopicArn"]   # idempotent


def _read_dotenv() -> dict:
    """Lit ROOT/.env (ignoré par Git) : c'est là que vit la clé d'API du LLM, jamais dans le code."""
    path, values = ROOT / ".env", {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                values[k.strip()] = v.strip().strip('"').strip("'")
    return values


def cmd_alerts_topic(args):
    sns, arn = session.client("sns"), _topic_arn()
    print(f"Sujet SNS : {arn}")
    existing = {s["Endpoint"] for p in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=arn)
                for s in p["Subscriptions"]}
    for email in args.email or []:
        if email in existing:
            print(f"Déjà abonné : {email}")
            continue
        sns.subscribe(TopicArn=arn, Protocol="email", Endpoint=email)
        print(f"Abonnement demandé : {email} -> cliquer sur « Confirm subscription » dans l'e-mail reçu")
    for p in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=arn):
        for s in p["Subscriptions"]:
            state = "en attente de confirmation" if s["SubscriptionArn"] == "PendingConfirmation" else "confirmé"
            print(f"  {s['Endpoint']} : {state}")


def _zip_agent() -> bytes:
    src = ROOT / "src" / "agent"
    files = sorted(f for f in src.glob("*.py") if not f.name.startswith("test"))
    if not (src / "handler.py").exists():
        sys.exit("src/agent/handler.py introuvable : faire d'abord 'git pull' pour récupérer le code de B")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, f.name)
    return buf.getvalue()


def cmd_deploy_agent(args):
    lam = session.client("lambda")
    dotenv = _read_dotenv()
    llm = {k: dotenv.get(k, os.environ.get(k, "")) for k in AGENT_ENV_KEYS}
    if not llm["LLM_API_KEY"]:
        print("⚠ LLM_API_KEY absent de .env : l'agent rédigera ses diagnostics avec les règles (sans LLM)")
    env = {"Variables": {**llm, "TOPIC_ARN": _topic_arn(), "BUCKET": bucket_name(),
                         "TICKETS_TABLE": "tickets", "STATE_TABLE": "machine_state"}}
    code = _zip_agent()
    try:
        lam.create_function(FunctionName=AGENT_LAMBDA, Runtime="python3.12", Role=lab_role_arn(),
                            Handler="handler.lambda_handler", Code={"ZipFile": code},
                            Timeout=30, MemorySize=256, Environment=env)
        print(f"Lambda créée : {AGENT_LAMBDA}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=AGENT_LAMBDA, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=AGENT_LAMBDA)
        lam.update_function_configuration(FunctionName=AGENT_LAMBDA, Environment=env, Timeout=30)
        print(f"Lambda mise à jour : {AGENT_LAMBDA}")
    lam.get_waiter("function_active_v2").wait(FunctionName=AGENT_LAMBDA)
    _agent_invoke_config()
    print("La clé d'API est dans les variables de la Lambda, pas dans le dépôt.")
    print("Relancer 'deploy-scoring' pour que la Lambda de scoring appelle l'agent.")


def _agent_invoke_config() -> None:
    """Appels asynchrones de l'agent : 1 seule nouvelle tentative, puis la DLQ (si elle existe)."""
    kwargs = {"FunctionName": AGENT_LAMBDA, "MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 3600}
    dlq = _queue_arn(AGENT_DLQ)
    if dlq:
        kwargs["DestinationConfig"] = {"OnFailure": {"Destination": dlq}}
    session.client("lambda").put_function_event_invoke_config(**kwargs)


def cmd_agent_test(args):
    """Appelle l'agent une fois avec une alerte réaliste (machine 3, veille de sa panne du 4 octobre)."""
    sys.path.insert(0, str(ROOT / "src" / "scoring"))
    sys.path.insert(0, str(ROOT / "src" / "common"))
    from features import FEATURE_COLUMNS, compute_features
    from state_store import from_item
    item = session.client("dynamodb").get_item(TableName="machine_state",
                                               Key={"machineID": {"N": str(args.machine)}}).get("Item")
    if not item:
        sys.exit(f"Machine {args.machine} absente de machine_state : lancer load-machines")
    state = from_item(item)
    # Même charge utile que la Lambda de scoring (contrat 4), avec les 30 vraies valeurs de la machine.
    payload = {"machineID": args.machine, "datetime": state.get("last_datetime"), "risk": 0.999,
               "threshold": 0.948, "run_id": f"test-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}",
               "features": dict(zip(FEATURE_COLUMNS, compute_features(state)))}
    resp = session.client("lambda").invoke(FunctionName=AGENT_LAMBDA, Payload=json.dumps(payload).encode(),
                                           LogType="Tail")
    print(resp["Payload"].read().decode())
    import base64
    print(base64.b64decode(resp["LogResult"]).decode()[-2500:])


# ------------------------------------------- Semaine 4 : fiabilité et alarmes

def cmd_reliability(args):
    lam = session.client("lambda")
    scoring_dlq, agent_dlq = _queue_arn(SCORING_DLQ, create=True), _queue_arn(AGENT_DLQ, create=True)
    print(f"DLQ prêtes : {SCORING_DLQ}, {AGENT_DLQ}")

    for m in lam.list_event_source_mappings(FunctionName=SCORING_LAMBDA)["EventSourceMappings"]:
        lam.update_event_source_mapping(UUID=m["UUID"], MaximumRetryAttempts=2, BisectBatchOnFunctionError=True,
                                        DestinationConfig={"OnFailure": {"Destination": scoring_dlq}})
        print("Déclencheur Kinesis -> scoring : 2 tentatives, puis DLQ")
    try:
        _agent_invoke_config()
        print("Agent : 1 nouvelle tentative, puis DLQ")
    except ClientError:
        print("Agent pas encore déployé : relancer 'reliability' après 'deploy-agent'")
    lam.put_function_event_invoke_config(FunctionName=SIM_LAMBDA, MaximumRetryAttempts=0)
    print("Simulateur : aucune nouvelle tentative (le lot suivant reprend à la même position)")

    topic = _topic_arn()
    cw = session.client("cloudwatch")
    alarms = [
        ("smartcnc-scoring-errors", "AWS/Lambda", "Errors", "FunctionName", SCORING_LAMBDA, "Sum", 1),
        ("smartcnc-simulator-errors", "AWS/Lambda", "Errors", "FunctionName", SIM_LAMBDA, "Sum", 1),
        ("smartcnc-agent-errors", "AWS/Lambda", "Errors", "FunctionName", AGENT_LAMBDA, "Sum", 1),
        # Retard de lecture : la Lambda de scoring n'arrive plus à suivre le flux (> 5 min).
        ("smartcnc-kinesis-lag", "AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", STREAM,
         "Maximum", 300_000),
        ("smartcnc-scoring-dlq", "AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", SCORING_DLQ,
         "Maximum", 1),
        ("smartcnc-agent-dlq", "AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", AGENT_DLQ,
         "Maximum", 1),
    ]
    for name, ns, metric, dim, value, stat, threshold in alarms:
        cw.put_metric_alarm(
            AlarmName=name, Namespace=ns, MetricName=metric, Dimensions=[{"Name": dim, "Value": value}],
            Statistic=stat, Period=300, EvaluationPeriods=1, Threshold=threshold,
            ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching",
            AlarmActions=[topic], AlarmDescription=f"SmartCNC : {metric} de {value}",
        )
        print(f"Alarme : {name} (seuil {threshold}) -> e-mail")
    print("Les alarmes partent sur le même sujet SNS que les alertes (objet « ALARM: … »).")


def cmd_alarms(args):
    resp = session.client("cloudwatch").describe_alarms(AlarmNamePrefix="smartcnc-")
    for a in resp["MetricAlarms"]:
        print(f"{a['AlarmName']:<28} {a['StateValue']:<18} {a.get('StateReason', '')[:70]}")


def _delete_prefix(prefix: str) -> int:
    s3, name, n = session.client("s3"), bucket_name(), 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=name, Delete={"Objects": keys})
            n += len(keys)
    return n


def cmd_cleanup(args):
    """Supprime ce qui ne sert plus (rien d'utile à la démo)."""
    ecr = session.client("ecr")
    try:
        old = ecr.list_images(repositoryName=SCORING_REPO, filter={"tagStatus": "UNTAGGED"})["imageIds"]
        try:   # jamais l'image que la Lambda utilise encore (poussée mais pas redéployée)
            used = session.client("lambda").get_function(FunctionName=SCORING_LAMBDA)["Code"].get("ResolvedImageUri", "")
            old = [i for i in old if not used.endswith(i["imageDigest"])]
        except ClientError:
            pass
        if old:
            ecr.batch_delete_image(repositoryName=SCORING_REPO, imageIds=old)
        print(f"ECR : {len(old)} anciennes images supprimées (seule 'latest' est gardée)")
    except ClientError as e:
        print("ECR :", e.response["Error"]["Code"])
    print(f"S3 athena-results/ : {_delete_prefix('athena-results/')} fichiers supprimés")
    print(f"S3 stream-errors/ : {_delete_prefix('stream-errors/')} fichiers supprimés")
    if args.old_runs:
        pages = session.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket_name(),
                                                                               Prefix="stream/risks/")
        runs = sorted({o["Key"].split("/")[2] for p in pages for o in p.get("Contents", [])})
        for run in runs[:-1]:
            print(f"S3 stream/risks/{run}/ : {_delete_prefix(f'stream/risks/{run}/')} fichiers supprimés")
        print(f"Rejeu gardé : {runs[-1] if runs else 'aucun'}")
    try:
        state = session.client("kinesis").describe_stream_summary(StreamName=STREAM)
        print("⚠ Kinesis est encore allumé : lancer 'stream-down' en fin de séance",
              state["StreamDescriptionSummary"]["StreamStatus"])
    except ClientError:
        print("Kinesis : absent ✓ (rien n'est facturé à l'heure)")


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
    for c in ["sim-start", "sim-stop", "sim-reset", "sim-once", "status",
              "push-scoring", "deploy-scoring", "risks", "athena-setup", "athena-report", "athena-check", "dashboard",
              "deploy-agent", "reliability", "alarms"]:
        sub.add_parser(c)
    sub.add_parser("alerts-topic").add_argument("--email", action="append",
                                                help="adresse à abonner (répéter l'option pour plusieurs)")
    sub.add_parser("agent-test").add_argument("--machine", type=int, default=3)
    sub.add_parser("cleanup").add_argument("--old-runs", action="store_true",
                                           help="supprime aussi les risques archivés des anciens rejeux")
    sub.add_parser("upload-model").add_argument("--models", default=str(ROOT / "models"))
    lg = sub.add_parser("logs")
    lg.add_argument("--name", choices=["scoring", "simulator", "agent"], default="scoring")
    lg.add_argument("--minutes", type=int, default=30)
    lg.add_argument("--lines", type=int, default=40)
    lm = sub.add_parser("load-machines")
    lm.add_argument("--replay", default="test", help="rejeu dont on lit le manifest (test ou full)")
    lm.add_argument("--data", default=str(ROOT / "data"))
    args = p.parse_args()
    globals()["cmd_" + args.cmd.replace("-", "_")](args)


if __name__ == "__main__":
    main()
