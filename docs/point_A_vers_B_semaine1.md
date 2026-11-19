# Point A → B — fin de semaine 1

Salut ! Voici où en est la partie A (données et flux), ce qui arrive en semaine 2, et ce dont j'ai besoin de toi.

---

## 1. Ce qui est fait (semaine 1, partie A)

**Démo A atteinte : les événements arrivent dans S3.**

```
CSV ──► fichiers de rejeu ──► S3 replay/ ──► Lambda simulateur ──► Kinesis ──► Firehose ──► S3 stream/events/
                                              (EventBridge, 1 lot/min)
```

| Élément | Nom AWS | Rôle |
|---|---|---|
| Bucket S3 | `smartcnc-851725398242` | `raw/` (les 5 CSV), `replay/`, `stream/`, `models/` (vide, pour toi) |
| Fichiers de rejeu | `replay/test/` | Octobre → décembre, **pannes exclues**, un fichier JSONL par jour. Version test : 10 machines, 1er–15 octobre |
| Table DynamoDB | `replay_state` | Position du rejeu (`id = "simulator"`) |
| Tables DynamoDB | `machine_state`, `tickets` | Créées vides, selon le contrat 3 |
| Lambda | `smartcnc-simulator` | Envoie 60 h simulées par appel vers Kinesis (3 mois ≈ 37 min) |
| Règle EventBridge | `smartcnc-simulator-every-minute` | Déclenche le simulateur chaque minute |
| Kinesis | `smartcnc-measurements` | 1 shard, clé = machineID. **Supprimé à chaque fin de séance** |
| Firehose | `smartcnc-archive` | Archive le flux dans `stream/events/AAAA/MM/JJ/HH/` |

**Code (dans le dépôt) :**
- `src/simulator/prepare_replay.py` : construit les fichiers de rejeu à partir des CSV.
- `src/simulator/handler.py` : la Lambda simulateur.
- `infra/smartcnc.py` : crée ou supprime toutes les ressources en une commande (`stream-up`, `stream-down`, `sim-once`, `status`…). Le mode d'emploi est dans le `README.md`.
- `tests/test_simulator.py` : 9 tests locaux (données simulées, sans AWS).

**Les CSV sont dans `s3://smartcnc-851725398242/raw/`**, si tu veux les lire depuis ton notebook SageMaker.

---

## 2. Trois précisions sur les contrats (à valider ensemble)

1. **Archive** : Firehose écrit dans `stream/events/` (et pas à la racine de `stream/`), pour laisser la place à `stream/risks/` et `stream/tickets/` en semaine 3.
2. **Doublons et désordre** : si un lot échoue, il est renvoyé. Un même événement peut donc arriver deux fois, ou un peu dans le désordre. Je le gère dans la Lambda de scoring : tri par datetime, puis dédoublonnage. **Pour `update_state` : même règle que le contrat, un événement plus ancien que le dernier traité est ignoré.**
3. **Semaine 3** : on ajoutera deux types d'événements archivés, `risk` et `ticket`. Le contrat 1 sera à compléter.

---

## 3. La suite (semaine 2 : « chaque machine a un risque à jour »)

**De mon côté (A) :**
- **Image conteneur Lambda avec scikit-learn**, poussée dans ECR. C'est le test de l'étape 0, pas encore fait. Plan B si ça ne passe pas : endpoint SageMaker allumé pendant les démos.
- **Lambda de scoring.** Pour chaque lot reçu de Kinesis, elle :
  - lit l'état de la machine dans `machine_state` ;
  - ajoute l'événement et garde les dernières 24 h ;
  - calcule le risque ;
  - réécrit l'état une seule fois par machine et par lot.
- **Script de chargement** de `PdM_machines.csv` dans `machine_state` (modèle, âge, et `last_maint` = dernière maintenance de chaque composant **avant le 1er octobre**).
- En attendant ton modèle, j'utilise un **faux modèle** (risque fixe), pour avancer sans te bloquer.

**Ensemble (fin de semaine 2) :** brancher ton `features.py` et ton modèle dans la Lambda de scoring.

---

## 4. Ce dont j'ai besoin de toi

| # | Besoin | Pourquoi | Quand |
|---|---|---|---|
| 1 | **La version exacte de scikit-learn** (ex. `scikit-learn==1.5.2`), à mettre dans `requirements.txt` | Un modèle enregistré avec une version peut ne pas se charger avec une autre. J'en ai besoin pour construire l'image Lambda | **Dès que possible : c'est bloquant** |
| 2 | **`src/common/features.py`** avec `update_state`, `compute_features` et `FEATURE_COLUMNS` (contrat 2) | La Lambda doit calculer exactement les mêmes caractéristiques que l'entraînement | Courant de semaine 2 (même une première version) |
| 3 | **`features.py` en pur Python + numpy si possible** (pandas évité dans `update_state` et `compute_features`) | La Lambda traite des centaines d'événements par lot : il faut que ce soit léger et rapide | Avec le point 2 |
| 4 | **`model.pkl`, la liste des colonnes et le seuil d'alerte** dans `models/` (ex. `models/model.pkl`, `models/columns.json`, `models/threshold.json`) | La Lambda les charge au démarrage | Fin de semaine 2 |
| 5 | Ton avis sur les 3 précisions de la partie 2 | Pour figer les contrats | Au prochain point de 10 min |

---

## 5. Une question à trancher : sur quel compte tourne la démo ?

Chacune a son propre Learner Lab, donc **deux comptes AWS séparés**. Tout le flux temps réel est sur mon compte. Deux options :
- **(a)** Tu entraînes sur ton compte, puis tu m'envoies `model.pkl`, les colonnes et le seuil (par le dépôt ou un fichier). Je les dépose dans mon `models/`.
- **(b)** Tu fais tout sur ton compte, et on recopie le modèle à la fin.

Je propose **(a)** : c'est plus simple, et le budget de la démo reste sur un seul compte. On en parle ?

---

Pour tester le flux toi-même, tout est dans le `README.md` (section « Partie A — semaine 1 »). N'oublie pas `stream-down` en fin de séance : Kinesis est facturé à l'heure.
