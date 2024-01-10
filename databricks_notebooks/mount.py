# Databricks notebook source
# MAGIC %md
# MAGIC # Create the mounting point

# COMMAND ----------

if not any(mount.mountPoint == "/mnt/dpe-project" for mount in dbutils.fs.mounts()):
  extra_configs = {

    
    "fs.azure.account.key.group3stockage.blob.core.windows.net": dbutils.secrets.get(scope = "group3-key-vault", key = "storage-account-key")
  }

  dbutils.fs.mount(
    source = "wasbs://dpe-project@group3stockage.blob.core.windows.net/",
    mount_point = "/mnt/dpe-project",
    extra_configs = extra_configs
    )

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md # Tables creation

# COMMAND ----------

# MAGIC %md schema creation, need to rename some columns to avoid special characters and specifie data types 

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType
schema = StructType([
    StructField("c0", IntegerType(), True),
    StructField("NDPE", StringType(), True),
    StructField("Configuration_installation_chauffage_n2", StringType(), True),
    StructField("Facteur_couverture_solaire_saisi", FloatType(), True),
    StructField("Surface_habitable_desservie_par_installation_ECS", FloatType(), True),
    StructField("EmissionGES_eclairage", FloatType(), True),
    StructField("Cage_escalier", StringType(), True),
    StructField("Conso_5usages_finale_energie_n2", FloatType(), True),
    StructField("Type_generateur_froid", StringType(), True),
    StructField("Type_emetteur_installation_chauffage_n2", StringType(), True),
    StructField("Surface_totale_capteurs_photovoltaique", FloatType(), True),
    StructField("Nom_commune_Brut", StringType(), True),
    StructField("Conso_chauffage_depensier_installation_chauffage_n1", FloatType(), True),
    StructField("Cout_chauffage_energie_n2", FloatType(), True),
    StructField("Emission_GES_chauffage_energie_n2", FloatType(), True),
    StructField("Code_INSEE_BAN", StringType(), True),
    StructField("Type_energie_n3", StringType(), True),
    StructField("Etiquette_GES", StringType(), True),
    StructField("Type_generateur_n1_installation_n2", StringType(), True),
    StructField("Codepostal_brut", IntegerType(), True),
    StructField("Description_generateur_chauffage_n2_installation_n2", StringType(), True),
    StructField("Facteur_couverture_solaire", FloatType(), True),
    StructField("Annee_construction", FloatType(), True),
    StructField("Classe_altitude", StringType(), True),
    StructField("Codepostal_BAN", FloatType(), True),
    StructField("Conso_5usages_m2e_finale", FloatType(), True),
    StructField("Conso_5usagese_finale", FloatType(), True),
    StructField("Etiquette_DPE", StringType(), True),
    StructField("Hauteur_sous_plafond", FloatType(), True),
    StructField("N_departement_BAN", StringType(), True),
    StructField("Qualite_isolation_enveloppe", StringType(), True),
    StructField("Qualite_isolation_menuiseries", StringType(), True),
    StructField("Qualite_isolation_murs", StringType(), True),
    StructField("Qualite_isolation_plancher_bas", StringType(), True),
    StructField("Qualite_isolation_plancher_haut_comble_amenage", StringType(), True),
    StructField("Qualite_isolation_plancher_haut_comble_perdu", StringType(), True),
    StructField("Qualite_isolation_plancher_haut_toit_terrase", StringType(), True),
    StructField("Surface_habitable_immeuble", FloatType(), True),
    StructField("Surface_habitable_logement", FloatType(), True),
    StructField("Type_batiment", StringType(), True)
])

# COMMAND ----------

# MAGIC %md database creation

# COMMAND ----------

db_name = "raw"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# COMMAND ----------

# MAGIC %md ## Test

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/dpe-project/raw/test.csv', header=True, schema=schema).drop("c0")
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.test_data")

# COMMAND ----------

# MAGIC %md ##  Train

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/dpe-project/raw/train.csv', header=True, schema=schema).drop("c0")
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.train_data")

# COMMAND ----------

# MAGIC %md ## Val

# COMMAND ----------

# MAGIC %md val.csv don't have an index column

# COMMAND ----------

val_schema = StructType([field for field in schema.fields if field.name != "c0"])

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/dpe-project/raw/val.csv', header=True, schema=val_schema)
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.val_data")

# COMMAND ----------

# MAGIC %md # DROP TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.train_data;
# MAGIC DROP TABLE IF EXISTS raw.test_data;
# MAGIC DROP TABLE IF EXISTS raw.val_data;

# COMMAND ----------


