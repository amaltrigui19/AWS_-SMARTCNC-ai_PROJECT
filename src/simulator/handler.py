"""Lambda simulateur : rejoue un lot d'heures vers Kinesis à chaque déclenchement EventBridge.

À chaque appel :
1. lit la position dans DynamoDB (table replay_state, id = "simulator") ;
   si elle n'existe pas, part du début indiqué par manifest.json ;
2. lit dans S3 les événements de [position, position + HOURS_PER_BATCH[ ;
3. les envoie à Kinesis (clé de partition = machineID en texte, un "\n" à la fin
   de chaque enregistrement pour que Firehose produise du JSON ligne par ligne) ;
4. avance la position, avec une condition qui empêche deux appels d'avancer en double.

Si l'envoi échoue après plusieurs tentatives, la position n'avance pas : le lot sera
rejoué au prochain appel (la Lambda de scoring ignore les événements déjà vus).

Variables d'environnement :
    BUCKET            bucket S3 du projet
    REPLAY_PREFIX     ex. "replay/test/" ou "replay/full/"
    STREAM_NAME       défaut "smartcnc-measurements"
    STATE_TABLE       défaut "replay_state"
    HOURS_PER_BATCH   défaut 60
    MAX_RECORDS_PER_SECOND  défaut 900 (limite d'un shard : 1 000/s et 1 Mo/s)
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
STATE_ID = "simulator"
PUT_RECORDS_MAX = 500          # limite de l'API PutRecords
MAX_BYTES_PER_SECOND = 900_000  # marge sous 1 Mo/s par shard
MAX_ATTEMPTS = 6

_clients: dict = {}


def _client(name: str):
    if name not in _clients:
        _clients[name] = boto3.client(name)
    return _clients[name]


def _config() -> dict:
    prefix = os.environ.get("REPLAY_PREFIX", "replay/full/")
    return {
        "bucket": os.environ["BUCKET"],
        "prefix": prefix if prefix.endswith("/") else prefix + "/",
        "stream": os.environ.get("STREAM_NAME", "smartcnc-measurements"),
        "table": os.environ.get("STATE_TABLE", "replay_state"),
        "hours": int(os.environ.get("HOURS_PER_BATCH", "60")),
        "rps": int(os.environ.get("MAX_RECORDS_PER_SECOND", "900")),
    }


# --------------------------------------------------------------------------- S3

def read_manifest(cfg: dict) -> dict:
    obj = _client("s3").get_object(Bucket=cfg["bucket"], Key=cfg["prefix"] + "manifest.json")
    return json.loads(obj["Body"].read())


def read_window(cfg: dict, start: datetime, end: datetime) -> list[str]:
    """Lignes JSON dont le datetime est dans [start, end[, dans l'ordre du fichier."""
    lo, hi = start.strftime(DATETIME_FORMAT), end.strftime(DATETIME_FORMAT)
    lines: list[str] = []
    day = start.date()
    last_day = (end - timedelta(seconds=1)).date()
    while day <= last_day:
        key = f"{cfg['prefix']}{day.isoformat()}.jsonl"
        try:
            body = _client("s3").get_object(Bucket=cfg["bucket"], Key=key)["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                raise
            body = ""  # jour sans événement
        for line in body.splitlines():
            if not line:
                continue
            dt = json.loads(line)["datetime"]
            if lo <= dt < hi:  # le format fixe permet la comparaison de chaînes
                lines.append(line)
        day += timedelta(days=1)
    return lines


# --------------------------------------------------------------------- DynamoDB

def get_position(cfg: dict) -> str | None:
    item = _client("dynamodb").get_item(
        TableName=cfg["table"], Key={"id": {"S": STATE_ID}}, ConsistentRead=True
    ).get("Item")
    return item["position"]["S"] if item else None


def set_position(cfg: dict, new: str, old: str | None) -> None:
    kwargs = {
        "TableName": cfg["table"],
        "Item": {"id": {"S": STATE_ID}, "position": {"S": new}},
    }
    if old is None:
        kwargs["ConditionExpression"] = "attribute_not_exists(id)"
    else:
        kwargs["ConditionExpression"] = "#p = :old"
        kwargs["ExpressionAttributeNames"] = {"#p": "position"}
        kwargs["ExpressionAttributeValues"] = {":old": {"S": old}}
    _client("dynamodb").put_item(**kwargs)


# ---------------------------------------------------------------------- Kinesis

def to_records(lines: list[str]) -> list[dict]:
    return [
        {"Data": (line + "\n").encode("utf-8"), "PartitionKey": str(json.loads(line)["machineID"])}
        for line in lines
    ]


def send_records(client, stream: str, records: list[dict], rps: int = 900,
                 sleep=time.sleep, clock=time.monotonic) -> int:
    """Envoie les enregistrements par paquets de 500 en respectant le débit d'un shard.

    Les enregistrements refusés (ex. ProvisionedThroughputExceeded) sont renvoyés
    avec un délai croissant. Lève RuntimeError s'il en reste après MAX_ATTEMPTS.
    Renvoie le nombre d'enregistrements acceptés.
    """
    sent = 0
    for i in range(0, len(records), PUT_RECORDS_MAX):
        pending = records[i:i + PUT_RECORDS_MAX]
        for attempt in range(MAX_ATTEMPTS):
            t0 = clock()
            resp = client.put_records(StreamName=stream, Records=pending)
            failed = [rec for rec, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
            sent += len(pending) - len(failed)
            # Étale l'envoi : len(pending) enregistrements doivent prendre au moins len/rps secondes.
            size = sum(len(r["Data"]) + len(r["PartitionKey"]) for r in pending)
            min_duration = max(len(pending) / rps, size / MAX_BYTES_PER_SECOND)
            elapsed = clock() - t0
            if elapsed < min_duration:
                sleep(min_duration - elapsed)
            if not failed:
                break
            logger.warning("%d enregistrements refusés (tentative %d)", len(failed), attempt + 1)
            pending = failed
            sleep(min(0.2 * 2 ** attempt, 5.0))
        else:
            raise RuntimeError(f"{len(pending)} enregistrements toujours refusés après {MAX_ATTEMPTS} tentatives")
    return sent


# ----------------------------------------------------------------------- Lambda

def lambda_handler(event, context):
    cfg = _config()
    manifest = read_manifest(cfg)
    end_of_replay = manifest["end"]

    position = get_position(cfg)
    start_str = position or manifest["start"]
    if start_str >= end_of_replay:
        logger.info("Rejeu terminé (position %s, fin %s)", start_str, end_of_replay)
        return {"status": "termine", "position": start_str}

    start = datetime.strptime(start_str, DATETIME_FORMAT)
    end = min(start + timedelta(hours=cfg["hours"]), datetime.strptime(end_of_replay, DATETIME_FORMAT))

    lines = read_window(cfg, start, end)
    sent = send_records(_client("kinesis"), cfg["stream"], to_records(lines), rps=cfg["rps"])

    new_position = end.strftime(DATETIME_FORMAT)
    try:
        set_position(cfg, new_position, position)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning("Position modifiée par un autre appel ; lot envoyé en double")
            return {"status": "conflit", "sent": sent}
        raise

    logger.info("Lot %s -> %s : %d événements envoyés", start_str, new_position, sent)
    return {"status": "ok", "from": start_str, "to": new_position, "sent": sent}
