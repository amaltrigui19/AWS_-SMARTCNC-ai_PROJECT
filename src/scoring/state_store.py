"""Lecture / écriture de l'état machine (contrat 3, table machine_state).

L'état est le dict de features.new_state, plus deux champs gérés par la Lambda :
    risk           dernier score (0 à 1)
    risk_datetime  heure simulée de ce score
    last_alert     date simulée de la dernière alerte (semaine 3)

DynamoDB n'accepte pas les float : on passe par Decimal à l'écriture et on revient
en int / float à la lecture.
"""
from __future__ import annotations

import json
from decimal import Decimal

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

_ser, _deser = TypeSerializer(), TypeDeserializer()


def _from_decimal(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, list):
        return [_from_decimal(v) for v in value]
    if isinstance(value, dict):
        return {k: _from_decimal(v) for k, v in value.items()}
    return value


def to_item(state: dict) -> dict:
    """dict Python -> item DynamoDB (format client bas niveau)."""
    clean = json.loads(json.dumps(state), parse_float=Decimal)
    return {k: _ser.serialize(v) for k, v in clean.items()}


def from_item(item: dict) -> dict:
    """item DynamoDB -> dict Python, identique à ce que features.py attend."""
    return _from_decimal({k: _deser.deserialize(v) for k, v in item.items()})


def batch_get_states(client, table: str, machine_ids) -> dict[int, dict]:
    """Lit l'état de plusieurs machines (100 clés max par appel, non traitées relancées)."""
    ids = sorted({int(m) for m in machine_ids})
    states: dict[int, dict] = {}
    for i in range(0, len(ids), 100):
        request = {table: {"Keys": [{"machineID": {"N": str(m)}} for m in ids[i:i + 100]],
                           "ConsistentRead": True}}
        while request:
            resp = client.batch_get_item(RequestItems=request)
            for item in resp["Responses"].get(table, []):
                state = from_item(item)
                states[int(state["machineID"])] = state
            request = resp.get("UnprocessedKeys") or None
    return states


def batch_put_states(client, table: str, states) -> None:
    """Écrit des états (25 par appel, non traités relancés)."""
    states = list(states)
    for i in range(0, len(states), 25):
        request = {table: [{"PutRequest": {"Item": to_item(s)}} for s in states[i:i + 25]]}
        while request:
            resp = client.batch_write_item(RequestItems=request)
            request = resp.get("UnprocessedItems") or None
