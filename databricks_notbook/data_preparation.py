# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd
from sklearn.utils import shuffle
from pyspark.sql.functions import when, col, mean, lit, count,expr
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

train_data = spark.read.table("raw.train_data")
test_data = spark.read.table("raw.test_data")

# COMMAND ----------

display(train_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column to keep

# COMMAND ----------

list_columns= [
    "EmissionGES_eclairage",
    "Codepostal_brut",
    "Etiquette_DPE",
    "Hauteur_sous_plafond",
    "Qualite_isolation_enveloppe",
    "Qualite_isolation_murs",
    "Qualite_isolation_plancher_bas",
    "Surface_habitable_logement",
    "Type_batiment"
]

# COMMAND ----------

train_data = train_data.select(list_columns)
test_data = test_data.select(list_columns)

# COMMAND ----------

display(train_data.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove null / nan value

# COMMAND ----------

train_data = train_data.withColumn("Qualite_isolation_enveloppe", when(col("Qualite_isolation_enveloppe").isNull(), "moyenne").otherwise(col("Qualite_isolation_enveloppe")))
train_data = train_data.withColumn("Qualite_isolation_murs", when(col("Qualite_isolation_murs").isNull(), "insuffisante").otherwise(col("Qualite_isolation_murs")))
train_data = train_data.withColumn("Qualite_isolation_plancher_bas", when(col("Qualite_isolation_plancher_bas").isNull(), "moyenne").otherwise(col("Qualite_isolation_plancher_bas")))

test_data = test_data.withColumn("Qualite_isolation_enveloppe", when(col("Qualite_isolation_enveloppe").isNull(), "moyenne").otherwise(col("Qualite_isolation_enveloppe")))
test_data = test_data.withColumn("Qualite_isolation_murs", when(col("Qualite_isolation_murs").isNull(), "insuffisante").otherwise(col("Qualite_isolation_murs")))
test_data = test_data.withColumn("Qualite_isolation_plancher_bas", when(col("Qualite_isolation_plancher_bas").isNull(), "moyenne").otherwise(col("Qualite_isolation_plancher_bas")))

# COMMAND ----------

window_spec = Window().rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

train_data = train_data.withColumn("Hauteur_sous_plafond", when(col("Hauteur_sous_plafond").isNull(), mean("Hauteur_sous_plafond").over(window_spec)).otherwise(col("Hauteur_sous_plafond")))
train_data = train_data.withColumn("Surface_habitable_logement", when(col("Surface_habitable_logement").isNull(), mean("Surface_habitable_logement").over(window_spec)).otherwise(col("Surface_habitable_logement")))
test_data = test_data.withColumn("Hauteur_sous_plafond", when(col("Hauteur_sous_plafond").isNull(), mean("Hauteur_sous_plafond").over(window_spec)).otherwise(col("Hauteur_sous_plafond")))
test_data = test_data.withColumn("Surface_habitable_logement", when(col("Surface_habitable_logement").isNull(), mean("Surface_habitable_logement").over(window_spec)).otherwise(col("Surface_habitable_logement")))

# COMMAND ----------

display(train_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utils function

# COMMAND ----------

columns = [
    "Qualite_isolation_enveloppe",
    "Qualite_isolation_murs",
    "Qualite_isolation_plancher_bas",
    "Type_batiment"
]
columns_to_categorical = [
    "Qualite_isolation_enveloppe",
    "Qualite_isolation_murs",
    "Qualite_isolation_plancher_bas",
    "Type_batiment"
]

# COMMAND ----------

def train_test_split(tr_data, te_data):
    # Assuming 'Etiquette_DPE' is the label column
    label_column = 'Etiquette_DPE'

    # Selecting features for training data
    X_train = tr_data.select([col(column) for column in tr_data.columns if column != label_column])
    y_train = tr_data.select(label_column)

    # Selecting features for testing data
    X_test = te_data.select([col(column) for column in te_data.columns if column != label_column])
    y_test = te_data.select(label_column)

    return X_train, y_train, X_test, y_test

# COMMAND ----------

def to_categorical(df, column, mapping):
    if column in mapping:
        expr_str = f"CASE {column} "
        for category, value in mapping[column].items():
            expr_str += f"WHEN '{category}' THEN {value} "
        expr_str += f"ELSE {column} END AS {column}"
        df = df.withColumn(column, expr(expr_str))
    return df

# COMMAND ----------

def replace_str(df):
    categories = {}
    
    for column in columns_to_categorical:
        categories[column] = df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
    
    mapping = {column: {category: i + 1 for i, category in enumerate(categories[column])} for column in columns_to_categorical}
    
    for column in columns_to_categorical:
        df = to_categorical(df, column, mapping)

    return df

# COMMAND ----------

train_data = replace_str(train_data)
test_data = replace_str(test_data)

# COMMAND ----------

display(train_data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save transformed data

# COMMAND ----------

db_name = "transformed"

# COMMAND ----------

number_of_sample = (
    train_data.groupBy('Etiquette_DPE')
    .agg(count('*').alias('count'))
    .agg({"count": "min"})
    .collect()[0][0]
)

print("Minimum Count Value:", number_of_sample)

window_spec = Window.partitionBy('Etiquette_DPE').orderBy(F.lit(1))
sampled_data = train_data.withColumn('row_number', F.row_number().over(window_spec))
sampled_data = sampled_data.filter(F.col('row_number') <= number_of_sample).drop('row_number')

# COMMAND ----------

display(sampled_data)

# COMMAND ----------

sampled_data.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.workmodel_whitout_conso_train_data")
