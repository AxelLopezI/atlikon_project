# Databricks notebook source
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable 

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
# MAGIC ## Define the tables

# COMMAND ----------

silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from orders silver table
# MAGIC

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

display(df_gold.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert operation

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    (
        df_gold.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )
else:
    gold_delta = DeltaTable.forName(spark, gold_table)

    gold_delta.alias("source").merge(
        source = df_gold.alias("gold"), 
        condition = "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging with parent company data 
# MAGIC We want data for monthly level but child data is on daily level

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")

display(df_child.limit(10))

# COMMAND ----------

df_monthly = (
    df_child
    # Get month start date (e.g., 2025-11-30 → 2025-11-01)
    .withColumn(
        "month_start", 
        F.trunc("date", "MM")
    )
    # Group at monthly grain by month_start + product_code + customer_code
    .groupBy(
        "month_start", 
        "product_code", 
        "customer_code"
    )
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )
    # Rename month_start back to 'date' to match target schema
    .withColumnRenamed("month_start", "date")
)

display(df_monthly.limit(10))

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("parent_gold").merge(
    source = df_monthly.alias("child_gold"), 
    condition = "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()