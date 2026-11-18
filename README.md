# SmartCNC AI — maintenance prédictive (Azure PdM, 100 % AWS)

Prédire si une machine tombera en panne dans les 24 h, à partir du jeu public Microsoft Azure PdM.
Les contrats entre A et B sont dans [contracts/](contracts/).

## Partie A — semaine 1 : les données circulent

Chaîne : `Lambda simulateur` → `Kinesis smartcnc-measurements` → `Firehose` → `S3 stream/events/`

| Fichier | Rôle |
|---|---|
| [src/simulator/prepare_replay.py](src/simulator/prepare_replay.py) | Construit le rejeu (oct.–déc., **pannes exclues**), un JSONL par jour + `manifest.json` |
| [src/simulator/handler.py](src/simulator/handler.py) | Lambda : 60 h simulées par appel → Kinesis, position dans `replay_state` |
| [infra/smartcnc.py](infra/smartcnc.py) | Création / suppression des ressources (boto3, rôle `LabRole`) |
| [tests/test_simulator.py](tests/test_simulator.py) | Tests locaux (données synthétiques + moto) |

### 0. Préparer le PC (une fois)

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q tests
```

Télécharger les 5 CSV Azure PdM depuis Kaggle et les mettre dans `data/`
(`PdM_telemetry.csv`, `PdM_errors.csv`, `PdM_maint.csv`, `PdM_failures.csv`, `PdM_machines.csv`).

À chaque séance du Learner Lab : copier les identifiants (AWS Details → AWS CLI) dans
`%USERPROFILE%\.aws\credentials`. Ils expirent à la fin de la séance.

### 1. Construire les fichiers de rejeu

```powershell
# Test : 10 machines, 2 semaines (règle quotidienne)
.venv\Scripts\python src/simulator/prepare_replay.py --out data/replay/test --machines 1-10 --start 2015-10-01 --end 2015-10-15
# Complet : 100 machines, octobre à décembre (pour les répétitions)
.venv\Scripts\python src/simulator/prepare_replay.py --out data/replay/full
```

### 2. Ressources permanentes (une fois)

```powershell
.venv\Scripts\python infra/smartcnc.py bucket
.venv\Scripts\python infra/smartcnc.py upload-raw
.venv\Scripts\python infra/smartcnc.py upload-replay --name test
.venv\Scripts\python infra/smartcnc.py tables
.venv\Scripts\python infra/smartcnc.py deploy-simulator --replay test
```

### 3. À chaque séance

```powershell
.venv\Scripts\python infra/smartcnc.py stream-up      # Kinesis + Firehose (facturé à l'heure !)
.venv\Scripts\python infra/smartcnc.py sim-reset      # repartir du 1er octobre
.venv\Scripts\python infra/smartcnc.py sim-once       # un lot tout de suite, pour vérifier
.venv\Scripts\python infra/smartcnc.py sim-start      # puis 1 lot par minute
.venv\Scripts\python infra/smartcnc.py status
# … en fin de séance, TOUJOURS :
.venv\Scripts\python infra/smartcnc.py stream-down    # arrête le simulateur, supprime Firehose et Kinesis
```

Les fichiers arrivent dans `s3://smartcnc-<compte>/stream/events/AAAA/MM/JJ/HH/` environ 60 s après l'envoi.

### Choix techniques

- **Un fichier par jour** dans `replay/<nom>/` : chaque appel ne lit que 3 petits fichiers, pas tout le trimestre.
- **`"\n"` après chaque enregistrement** : Firehose colle les enregistrements tels quels, et sans retour à la ligne Athena ne peut pas lire le JSON.
- **Débit limité à 900 enregistrements/s** (un shard accepte 1 000/s et 1 Mo/s). Les enregistrements refusés sont renvoyés avec un délai croissant. Un lot complet (100 machines × 60 h ≈ 6 100 événements) prend environ 7 s.
- **Position avancée seulement après un envoi réussi**, avec une écriture conditionnelle. La Lambda a une concurrence réservée de 1 et aucune nouvelle tentative automatique, donc un lot en échec est repris par l'appel suivant.
- **À savoir pour la Lambda de scoring (semaine 2)** : un lot rejoué peut arriver **en double**, et les enregistrements renvoyés peuvent arriver **dans le désordre**. Il faut trier chaque lot par datetime et dédoublonner les événements de même datetime.
- Firehose range les fichiers par **heure d'arrivée**, pas par heure simulée. Le préfixe `stream/events/` laisse de la place pour `stream/risks/` et `stream/tickets/` en semaine 3.
