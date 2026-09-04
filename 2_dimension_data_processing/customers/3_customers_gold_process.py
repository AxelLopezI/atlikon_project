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
# MAGIC ## Read from customers bronze table 

# COMMAND ----------

df_silver = (
    spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
)

# COMMAND ----------

df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

display(df_gold.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in gold table

# COMMAND ----------

(
    df_gold
    .write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of gold table

# COMMAND ----------

query = f"SELECT * FROM {catalog}.{gold_schema}.sb_dim_{data_source} LIMIT 10;"

df_check = spark.sql(query)

display(df_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert operation with final dim table

# COMMAND ----------

# Rename customer_id column
df_sb_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers") 

delta_table.alias("target").merge(
    source = df_sb_customers.alias("source"), 
    condition = "target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()