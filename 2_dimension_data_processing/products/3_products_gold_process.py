# Databricks notebook source
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from delta.tables import DeltaTable 

# COMMAND ----------

# MAGIC %run ../../1_setup/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from products bronze table 

# COMMAND ----------

df_silver = (
    spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final columns

# COMMAND ----------

df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")

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

df_sb_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")

display(df_sb_products.limit(10))

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")

delta_table.alias("target").merge(
    source = df_sb_products.alias("source"),
    condition = "target.product_code = source.product_code"
).whenMatchedUpdate(
    set = {
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values = {
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()