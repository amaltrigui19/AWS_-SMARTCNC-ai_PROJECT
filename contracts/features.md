# Contrat 2 — features.py

Fichier unique : src/common/features.py, utilisé par le training job ET la Lambda de scoring.

Fonctions :
- update_state(state: dict, event: dict) -> dict
  Ajoute l'événement à l'état et ne garde que les 24 dernières heures.
- compute_features(state: dict) -> list[float]
  Renvoie le vecteur dans l'ordre exact de FEATURE_COLUMNS.
- FEATURE_COLUMNS: list[str]  (ordre figé, exporté avec le modèle)

Caractéristiques (30) :
| Groupe      | Colonnes                                             | Nb |
|-------------|------------------------------------------------------|----|
| Capteurs    | {volt,rotate,pressure,vibration}_{mean,std}_{3h,24h} | 16 |
| Erreurs     | error1_count_24h … error5_count_24h                  | 5  |
| Maintenance | comp1_days_since … comp4_days_since                  | 4  |
| Machine     | model_model1 … model_model4 (one-hot), age (années)  | 5  |

Règles :
- Moins de 3 h d'historique : écart-type = 0.
- Initialisation : last_maint contient, pour chaque composant, la dernière
  maintenance AVANT le début de la période (PdM_maint.csv remonte à juin 2014).
- Composant sans aucune maintenance connue : jours depuis le 2014-06-01.
- Une maintenance à la même heure qu'une panne est une réparation après panne :
  elle n'est jamais utilisée pour prédire cette panne.
- Version scikit-learn identique partout (voir requirements.txt).