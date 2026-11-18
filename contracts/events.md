# Contrat 1 — Format des événements Kinesis

Flux : smartcnc-measurements · Clé de partition : machineID (en texte, ex. "17")
Encodage : JSON UTF-8, un événement par enregistrement
datetime : même format que les CSV, "YYYY-MM-DD HH:MM:SS" (avec un espace, sans fuseau)

| type      | Champs obligatoires                                                        |
|-----------|----------------------------------------------------------------------------|
| telemetry | type, machineID (int), datetime, volt, rotate, pressure, vibration (float) |
| error     | type, machineID (int), datetime, errorID ("error1" à "error5")             |
| maint     | type, machineID (int), datetime, comp ("comp1" à "comp4")                  |

Exemple :
{"type": "telemetry", "machineID": 17, "datetime": "2015-10-03 14:00:00", "volt": 171.2, "rotate": 448.9, "pressure": 101.3, "vibration": 40.1}

Règles :
- Les pannes (PdM_failures.csv) ne sont JAMAIS envoyées : elles servent de vérité terrain.
- Un événement plus ancien que le dernier traité pour sa machine est ignoré.
- Un type inconnu est ignoré et journalisé.