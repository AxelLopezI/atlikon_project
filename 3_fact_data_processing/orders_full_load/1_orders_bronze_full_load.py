# Databricks notebook source
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../../1_setup/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure S3 bucket path

# COMMAND ----------

base_path = f's3://atlikon-dp/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

print(f"Base_path: {base_path}")
print(f"Landing path: {landing_path}")
print(f"Processed path: {processed_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the table

# COMMAND ----------

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read files from landing path

# COMMAND ----------

df = (
    spark.read
    .options(header='True', inferSchema='True')
    .csv(f"{landing_path}/*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

print("Total rows: ", df.count())
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in bronze table

# COMMAND ----------

(
    df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("append")
    .saveAsTable(bronze_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of bronze table

# COMMAND ----------

query = f"SELECT * FROM {bronze_table} LIMIT 10;"

df_check = spark.sql(query)

display(df_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move files from landing to processed

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file_info in files:
    dbutils.fs.mv(
        file_info.path, 
        f"{processed_path}/{file_info.name}",
        True
    )