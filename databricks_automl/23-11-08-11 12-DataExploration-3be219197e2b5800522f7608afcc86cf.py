# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC - This notebook performs exploratory data analysis on the dataset.
# MAGIC - To expand on the analysis, attach this notebook to a cluster with runtime version **14.1.x-cpu-ml-scala2.12**,
# MAGIC edit [the options of pandas-profiling](https://pandas-profiling.ydata.ai/docs/master/rtd/pages/advanced_usage.html), and rerun it.
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/98374659829270).

# COMMAND ----------

import mlflow
import os
import uuid
import shutil
import pandas as pd
import databricks.automl_runtime

# Download input data from mlflow into a pandas DataFrame
# Create temporary directory to download data
temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(temp_dir)

# Download the artifact and read it
training_data_path = mlflow.artifacts.download_artifacts(run_id="51d3cd31ac344cdebd44b36c015dad00", artifact_path="data", dst_path=temp_dir)
df = pd.read_parquet(os.path.join(training_data_path, "training_data"))

# Delete the temporary data
shutil.rmtree(temp_dir)

target_col = "Etiquette_DPE"

# Drop columns created by AutoML before pandas-profiling
df = df.drop(['_automl_split_col_0000'], axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Truncate rows
# MAGIC Only the first 10000 rows will be considered for pandas-profiling to avoid out-of-memory issues.
# MAGIC Comment out next cell and rerun the notebook to profile the full dataset.

# COMMAND ----------

df = df.iloc[:10000, :]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Results

# COMMAND ----------

from ydata_profiling import ProfileReport
df_profile = ProfileReport(df, minimal=True, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)
