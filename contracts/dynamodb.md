# Contrat 3 — Tables DynamoDB (mode à la demande)

## machine_state — clé de partition : machineID (Number)
| Attribut      | Type | Contenu                                                  |
|---------------|------|----------------------------------------------------------|
| model         | S    | "model1" à "model4" (chargé une fois)                    |
| age           | N    | Âge en années (chargé une fois)                          |
| telemetry_24h | L    | 24 dernières mesures                                     |
| errors_24h    | L    | Erreurs des 24 dernières heures                          |
| last_maint    | M    | {"comp1": datetime, …, "comp4": datetime}, initialisé avec la dernière maintenance avant le rejeu |
| last_datetime | S    | Dernier événement traité ("YYYY-MM-DD HH:MM:SS")         |
| risk          | N    | Dernier score (0 à 1)                                    |
| last_alert    | S    | Date simulée de la dernière alerte (règle des 12 h)      |

## tickets — clé de partition : ticketID (String, UUID)
| Attribut       | Type | Contenu                               |
|----------------|------|---------------------------------------|
| machineID      | N    |                                       |
| created_at     | S    | Date simulée de l'alerte              |
| risk           | N    | Score au moment de l'alerte           |
| suspected_comp | S    | "comp1" à "comp4" ou "inconnu"        |
| recommendation | S    | Texte rédigé par l'agent Bedrock      |
| status         | S    | "ouvert", "en cours", "fermé"         |

## replay_state — clé de partition : id (String) = "simulator"
| Attribut | Type | Contenu                                         |
|----------|------|-------------------------------------------------|
| position | S    | Prochain datetime à rejouer ("YYYY-MM-DD HH:MM:SS") |