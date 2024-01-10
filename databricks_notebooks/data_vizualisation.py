# Databricks notebook source
# DBTITLE 1,Import
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Read data
train_df = spark.read.table("raw.test_data")

# COMMAND ----------

# DBTITLE 1,Get percentage of missing values
total_rows = train_df.count()
missing_percentages = [(col_name, (1 - (train_df.where(col(col_name).isNotNull()).count() / total_rows)) * 100)
                       for col_name in train_df.columns]

result_df = spark.createDataFrame(missing_percentages, ["Column", "Missing Percentage"])

result_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Remove useless column 
limit = 20

columns_to_drop = [row["Column"] for row in result_df.collect() if row["Missing Percentage"] > limit]

df_filtered = train_df.drop(*columns_to_drop)

display(df_filtered)

# COMMAND ----------

display(train_df)

# COMMAND ----------


