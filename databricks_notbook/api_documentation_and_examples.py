# Databricks notebook source
# MAGIC %md
# MAGIC # API documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## endpoint
# MAGIC `https://adb-8310178201993138.18.azuredatabricks.net/model/api_debug/6/invocations`

# COMMAND ----------

# MAGIC %md
# MAGIC ## header
# MAGIC ```python
# MAGIC headers = {'Authorization': f'Bearer <your-token>}', 'Content-Type': 'application/json'}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## exemple de donnée d'entrée :
# MAGIC ```python
# MAGIC {
# MAGIC   "columns": [
# MAGIC     "EmissionGES_eclairage",
# MAGIC     "Codepostal_brut",
# MAGIC     "Classe_altitude",
# MAGIC     "Conso_5usages_m2e_finale",
# MAGIC     "Conso_5usagese_finale",
# MAGIC     "Hauteur_sous_plafond",
# MAGIC     "Qualite_isolation_enveloppe",
# MAGIC     "Qualite_isolation_menuiseries",
# MAGIC     "Qualite_isolation_murs",
# MAGIC     "Qualite_isolation_plancher_bas",
# MAGIC     "Surface_habitable_logement",
# MAGIC     "Type_batiment",
# MAGIC   ],
# MAGIC   "data": [
# MAGIC     [15.0, 29550, "infu00e9rieur u00e0 400m", 398.0, 45860.30078125, 2.299999952316284, "insuffisante", "bonne", "insuffisante", "insuffisante", 115.19999694824219, "maison"],
# MAGIC     [3.9000000953674316, 92400, "infu00e9rieur u00e0 400m", 227.0, 6768.7001953125, 2.5, "insuffisante", "moyenne", "insuffisante", "tru00e8s bonne", 29.799999237060547, "appartement"],
# MAGIC     [15.600000381469727, 85120, "infu00e9rieur u00e0 400m", 916.0, 107408.6015625, 2.5, "bonne", "insuffisante", "moyenne", "insuffisante", 117.30000305175781, "maison"],
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC On précise le nom des colonnes et les données, ici on a 3 exemples. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Exemple de réponse
# MAGIC
# MAGIC `["G","G","G"]`
# MAGIC
# MAGIC On resoit bien 3 predictions

# COMMAND ----------

# MAGIC %md
# MAGIC # Test

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = 'https://adb-8310178201993138.18.azuredatabricks.net/model/api_debug/6/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
  ds_dict = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()


# COMMAND ----------

data = {
  "columns": [
    "EmissionGES_eclairage",
    "Codepostal_brut",
    "Classe_altitude",
    "Conso_5usages_m2e_finale",
    "Conso_5usagese_finale",
    "Hauteur_sous_plafond",
    "Qualite_isolation_enveloppe",
    "Qualite_isolation_menuiseries",
    "Qualite_isolation_murs",
    "Qualite_isolation_plancher_bas",
    "Surface_habitable_logement",
    "Type_batiment",
  ],
  "data": [
    [15.0, 29550, "infu00e9rieur u00e0 400m", 398.0, 45860.30078125, 2.299999952316284, "insuffisante", "bonne", "insuffisante", "insuffisante", 115.19999694824219, "maison"],
    [3.9000000953674316, 92400, "infu00e9rieur u00e0 400m", 227.0, 6768.7001953125, 2.5, "insuffisante", "moyenne", "insuffisante", "tru00e8s bonne", 29.799999237060547, "appartement"],
    [15.600000381469727, 85120, "infu00e9rieur u00e0 400m", 916.0, 107408.6015625, 2.5, "bonne", "insuffisante", "moyenne", "insuffisante", 117.30000305175781, "maison"],
  ]
}

# COMMAND ----------

score_model(data)

# COMMAND ----------


