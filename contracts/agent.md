# Contrat 4 — Appel de l'agent et archives (semaine 3) — proposition de A, à valider par B

## Qui fait quoi

| Étape | Responsable |
|---|---|
| Calcul du risque, règle des 12 h, mise à jour de `last_alert` dans `machine_state` | Lambda de scoring (A) |
| Appel de l'agent (asynchrone) | Lambda de scoring (A) |
| Outils, prompt Bedrock, ticket dans `tickets`, e-mail SNS, archive du ticket | Lambda agent (B) |

**L'agent ne modifie jamais `machine_state`** : la Lambda de scoring réécrit l'état complet à chaque lot,
donc toute écriture de l'agent serait écrasée. L'agent peut lire `machine_state` (outils « erreurs récentes »,
« historique de maintenance », « fiche machine »).

## Règle des 12 h (dans la Lambda de scoring)

Une alerte part si `risk >= threshold` et si `last_alert` est vide ou date d'au moins
`cooldown_hours` (12, lu dans `threshold.json`) en heures **simulées**. `last_alert` est alors mis à jour.
Si l'appel de l'agent échoue, `last_alert` n'est pas modifié (l'alerte sera retentée à l'heure suivante).

## Appel de l'agent

- Fonction : `smartcnc-agent` (variable `AGENT_FUNCTION` de la Lambda de scoring ; vide = pas d'appel, alerte seulement journalisée)
- Type : `InvocationType="Event"` (asynchrone, la Lambda de scoring n'attend pas Bedrock)
- Charge utile (JSON) :

```json
{
  "machineID": 3,
  "datetime": "2015-10-03 11:00:00",
  "risk": 0.9993,
  "threshold": 0.948,
  "run_id": "20261124T101500",
  "features": {"volt_mean_3h": 171.2, "...": 0.0, "error2_count_24h": 1.0, "comp2_days_since": 45.0, "age": 8.0}
}
```

`features` contient les 30 valeurs de `FEATURE_COLUMNS`, par nom : l'agent peut citer les signaux
(ex. erreurs des dernières 24 h, composant le plus ancien) sans les recalculer.

## Archives dans S3 (lues par Athena)

| Préfixe | Écrit par | Une ligne JSON par… | Champs |
|---|---|---|---|
| `stream/events/` | Firehose (A) | événement rejoué | contrat 1 |
| `stream/risks/run=<run_id>/` | Lambda de scoring (A) | mesure scorée | `run_id`, `machineID`, `datetime`, `risk`, `threshold`, `above`, `alert` |
| `stream/tickets/` | Lambda agent (B) | ticket | contrat 3 (`ticketID`, `machineID`, `created_at`, `risk`, `suspected_comp`, `recommendation`, `status`) + `run_id` |

Format : JSON, un objet par ligne, encodage UTF-8, dates `"YYYY-MM-DD HH:MM:SS"` (contrat 1).

`run_id` identifie un rejeu (fixé par `load-machines`) : Athena compare les alertes d'un même rejeu aux pannes réelles.
