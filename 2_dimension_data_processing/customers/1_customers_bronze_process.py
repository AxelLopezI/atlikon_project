# Databricks notebook source
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable 

# COMMAND ----------

# MAGIC %run /Workspace/Users/axl.dxn@gmail.com/atlikon_pipeline/1_setup/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure S3 bucket path

# COMMAND ----------

base_path = f's3://atlikon-dp/{data_source}/*.csv'
print(f"base_path: {base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DataFrame with raw data and metadata

# COMMAND ----------

df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of DataFrame

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in bronze table

# COMMAND ----------

(
    df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of bronze table

# COMMAND ----------

query = f"SELECT * FROM {catalog}.{bronze_schema}.{data_source} LIMIT 10;"

df_check = spark.sql(query)

display(df_check.limit(10))