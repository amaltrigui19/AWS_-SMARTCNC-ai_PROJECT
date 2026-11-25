"""Lambda agent SmartCNC AI (contrat 4, voir contracts/agent.md).

Appelée en asynchrone par la Lambda de scoring, une fois par alerte :
  1. lit la machine dans machine_state avec 3 outils (lecture seule) ;
  2. demande à Bedrock le composant suspecté et une recommandation ;
  3. crée le ticket dans DynamoDB (contrat 3) ;
  4. envoie l'e-mail SNS ;
  5. archive le ticket dans S3 (stream/tickets/) pour Athena.

L'agent n'écrit JAMAIS dans machine_state.

Variables d'environnement : MODEL_ID (Bedrock, optionnel), LLM_API_KEY / LLM_BASE_URL / LLM_MODEL (API LLM
compatible OpenAI, optionnelle ; LLM_MAX_TOKENS, LLM_REASONING_EFFORT facultatifs), TOPIC_ARN, BUCKET, TICKETS_TABLE, STATE_TABLE.
Uniquement boto3 et la bibliothèque standard (simple zip, sans Docker).
Ordre des diagnostics : Bedrock, puis API LLM, puis règles (toujours disponibles).
"""
import json
import logging
import os
import re
import urllib.request
import uuid
from datetime import datetime
from decimal import Decimal

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

COMPONENTS = ("comp1", "comp2", "comp3", "comp4")
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# Nombre d'heures d'avance annoncé : même horizon que la cible failure_24h.
HORIZON_H = 24

_clients = {}


def _client(name):
    """Clients boto3 créés une fois (et remplaçables dans les tests)."""
    if name not in _clients:
        _clients[name] = boto3.client(name)
    return _clients[name]


def _resource():
    if "dynamodb" not in _clients:
        _clients["dynamodb"] = boto3.resource("dynamodb")
    return _clients["dynamodb"]


def _plain(obj):
    """Decimal (DynamoDB) -> int/float, pour json et pour le prompt."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    if isinstance(obj, dict):
        return {k: _plain(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_plain(v) for v in obj]
    return obj


# ------------------------------------------------------------ les 3 outils
# Lecture seule dans machine_state.

def _get_state(machine_id):
    table = _resource().Table(os.environ["STATE_TABLE"])
    item = table.get_item(Key={"machineID": int(machine_id)}).get("Item")
    return _plain(item) if item else {}


def tool_recent_errors(state):
    """Outil 1 : erreurs des dernières 24 h ([[datetime, errorID], ...])."""
    return [{"datetime": e[0], "error": e[1]} for e in state.get("errors_24h", [])]


def tool_maintenance_history(state):
    """Outil 2 : dernière maintenance de chaque composant."""
    last = state.get("last_maint", {})
    return {c: last.get(c) for c in COMPONENTS}


def tool_machine_sheet(state):
    """Outil 3 : fiche machine (modèle et âge en années)."""
    return {"model": state.get("model"), "age": state.get("age")}


# ----------------------------------------------------------------- prompt

def _days_since(features):
    return {c: features.get(f"{c}_days_since") for c in COMPONENTS}


def fallback_component(features):
    """Composant sans maintenance depuis le plus longtemps."""
    days = {c: v for c, v in _days_since(features).items() if v is not None}
    return max(days, key=days.get) if days else "inconnu"


def _sensor_drift(features):
    """Capteur dont la moyenne des 3 dernières heures s'écarte le plus de celle de 24 h (en %)."""
    best, best_pct = None, 0.0
    for s in ("volt", "rotate", "pressure", "vibration"):
        m3, m24 = features.get(f"{s}_mean_3h"), features.get(f"{s}_mean_24h")
        if m3 is None or not m24:
            continue
        pct = (m3 - m24) / m24 * 100
        if abs(pct) > abs(best_pct):
            best, best_pct = s, pct
    return best, best_pct


def rule_based_diagnosis(event, errors, maint):
    """Diagnostic sans IA générative : gratuit, déterministe, toujours disponible."""
    f = event.get("features", {})
    comp = fallback_component(f)
    days = _days_since(f).get(comp)
    parts = [f"Risque de panne de {float(event['risk']):.0%} dans les {HORIZON_H} prochaines heures."]
    if days is not None:
        last = maint.get(comp)
        parts.append(f"Le composant {comp} n'a pas été remplacé depuis {days:.0f} jours"
                     + (f" (dernière maintenance : {last})." if last else "."))
    if errors:
        kinds = sorted({e["error"] for e in errors})
        parts.append(f"{len(errors)} erreur(s) ces dernières 24 h ({', '.join(kinds)}).")
    sensor, pct = _sensor_drift(f)
    if sensor and abs(pct) >= 1:
        parts.append(f"Le capteur {sensor} dérive de {pct:+.1f} % sur les 3 dernières heures.")
    parts.append(f"Recommandation : inspecter {comp} et planifier une maintenance avant la panne.")
    return comp, " ".join(parts)


def build_prompt(event, errors, maint, sheet):
    f = event.get("features", {})
    counts = {k: f[k] for k in sorted(f) if k.endswith("_count_24h") and f[k]}
    sensors = {
        s: {"moyenne_3h": round(f.get(f"{s}_mean_3h", 0), 2),
            "moyenne_24h": round(f.get(f"{s}_mean_24h", 0), 2)}
        for s in ("volt", "rotate", "pressure", "vibration")
    }
    facts = {
        "machine": event["machineID"],
        "date_alerte": event["datetime"],
        "risque_panne_24h": round(float(event["risk"]), 4),
        "seuil": event["threshold"],
        "fiche": sheet,
        "erreurs_24h": errors,
        "compteurs_erreurs_24h": counts,
        "derniere_maintenance_par_composant": maint,
        "jours_depuis_maintenance": _days_since(f),
        "capteurs": sensors,
    }
    return (
        "Tu es l'assistant de maintenance d'une usine. Un modèle de prédiction annonce une "
        f"panne probable de cette machine dans les {HORIZON_H} prochaines heures.\n"
        "Voici les données (JSON) :\n"
        f"{json.dumps(facts, ensure_ascii=False)}\n\n"
        "Réponds UNIQUEMENT par un objet JSON, sans autre texte, de la forme :\n"
        '{"suspected_comp": "comp1|comp2|comp3|comp4|inconnu", '
        '"recommendation": "2 à 3 phrases en français : quoi vérifier et pourquoi"}\n'
        "Appuie-toi seulement sur les données fournies."
    )


def ask_bedrock(prompt):
    resp = _client("bedrock-runtime").converse(
        modelId=os.environ["MODEL_ID"],
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 400, "temperature": 0.2},
    )
    return resp["output"]["message"]["content"][0]["text"]


def parse_answer(text):
    """Extrait {"suspected_comp", "recommendation"} de la réponse du modèle."""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("pas de JSON dans la réponse")
    data = json.loads(match.group(0))
    comp = str(data.get("suspected_comp", "inconnu")).strip().lower()
    if comp not in COMPONENTS:
        comp = "inconnu"
    reco = str(data.get("recommendation", "")).strip()
    if not reco:
        raise ValueError("recommandation vide")
    return comp, reco


def ask_llm_api(prompt):
    """API LLM compatible OpenAI (Groq, Gemini, etc.) avec la bibliothèque standard."""
    url = os.environ["LLM_BASE_URL"].rstrip("/") + "/chat/completions"
    payload = {
        "model": os.environ["LLM_MODEL"],
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        # Les modèles qui « réfléchissent » (Gemini) consomment cette limite : large par défaut.
        "max_tokens": int(os.environ.get("LLM_MAX_TOKENS", "1500")),
    }
    if os.environ.get("LLM_REASONING_EFFORT"):   # ex. "low" pour Gemini
        payload["reasoning_effort"] = os.environ["LLM_REASONING_EFFORT"]
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + os.environ["LLM_API_KEY"],
    })
    with urllib.request.urlopen(req, timeout=int(os.environ.get("LLM_TIMEOUT", "10"))) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"]


def diagnose(event, errors, maint, sheet):
    """(composant, recommandation). Ne plante jamais.

    Ordre : Bedrock (si MODEL_ID), puis API LLM (si LLM_API_KEY), puis règles.
    Chaque échec passe au suivant ; les règles sont toujours disponibles.
    """
    providers = []
    if os.environ.get("MODEL_ID"):
        providers.append(("Bedrock", ask_bedrock))
    if os.environ.get("LLM_API_KEY") and os.environ.get("LLM_BASE_URL") and os.environ.get("LLM_MODEL"):
        providers.append(("API LLM", ask_llm_api))
    if providers:
        prompt = build_prompt(event, errors, maint, sheet)
        for name, ask in providers:
            try:
                return parse_answer(ask(prompt))
            except Exception as exc:  # accès refusé, quota, réseau, réponse illisible...
                log.warning("%s indisponible, essai suivant : %s", name, exc)
    return rule_based_diagnosis(event, errors, maint)


# ------------------------------------------------- ticket, e-mail, archive

def ticket_id(event):
    """Identifiant stable : une alerte rejouée par Lambda (réessai) ne crée pas un 2e ticket."""
    key = f"{event.get('run_id', '')}|{event['machineID']}|{event['datetime']}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def create_ticket(event, comp, reco):
    """Écrit le ticket (contrat 3). Renvoie None si le ticket existe déjà."""
    ticket = {
        "ticketID": ticket_id(event),
        "machineID": int(event["machineID"]),
        "created_at": event["datetime"],
        "risk": Decimal(str(round(float(event["risk"]), 4))),
        "suspected_comp": comp,
        "recommendation": reco,
        "status": "ouvert",
    }
    table = _resource().Table(os.environ["TICKETS_TABLE"])
    try:
        table.put_item(Item=ticket, ConditionExpression="attribute_not_exists(ticketID)")
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return None
    return ticket


def send_email(ticket):
    risk = float(ticket["risk"])
    subject = (f"[SmartCNC] Machine {ticket['machineID']} : risque de panne "
               f"({risk:.0%}) - {ticket['suspected_comp']}")
    body = (
        f"Machine : {ticket['machineID']}\n"
        f"Date de l'alerte : {ticket['created_at']}\n"
        f"Risque de panne sous {HORIZON_H} h : {risk:.1%}\n"
        f"Composant suspecté : {ticket['suspected_comp']}\n\n"
        f"Recommandation :\n{ticket['recommendation']}\n\n"
        f"Ticket : {ticket['ticketID']} (statut : {ticket['status']})\n"
    )
    _client("sns").publish(TopicArn=os.environ["TOPIC_ARN"], Subject=subject[:100], Message=body)


def archive_ticket(ticket, run_id):
    """Une ligne JSON dans stream/tickets/ (contrat 4), lue par Athena."""
    row = _plain(dict(ticket))
    row["run_id"] = run_id
    key = f"stream/tickets/run={run_id}/{ticket['ticketID']}.json"
    _client("s3").put_object(
        Bucket=os.environ["BUCKET"], Key=key,
        Body=(json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"),
    )
    return key


# ---------------------------------------------------------------- handler

def lambda_handler(event, context=None):
    for field in ("machineID", "datetime", "risk", "threshold"):
        if field not in event:
            raise ValueError(f"champ manquant dans l'alerte : {field}")
    datetime.strptime(event["datetime"], DATETIME_FORMAT)  # format du contrat 1

    state = _get_state(event["machineID"])
    errors = tool_recent_errors(state)
    maint = tool_maintenance_history(state)
    sheet = tool_machine_sheet(state)

    comp, reco = diagnose(event, errors, maint, sheet)
    ticket = create_ticket(event, comp, reco)
    if ticket is None:
        log.info("Ticket déjà créé pour %s, rien à refaire", event["datetime"])
        return {"status": "doublon", "ticketID": ticket_id(event)}

    send_email(ticket)
    archive_ticket(ticket, event.get("run_id", "inconnu"))
    log.info("Ticket %s créé (machine %s, %s)", ticket["ticketID"], ticket["machineID"], comp)
    return {"status": "ok", "ticketID": ticket["ticketID"], "suspected_comp": comp}