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
# MAGIC ## Read from orders silver staging table
# MAGIC

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

display(df_gold.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert operation (sb_)

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

df_child = spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

incremental_month_df = (
    df_child
    .select(F.trunc("date", "MM").alias("start_month")).distinct()
)

display(incremental_month_df.limit(10))

incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
    """
)

print("Total rows: ", monthly_table.count())

display(monthly_table.limit(10))

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn(
        "month_start", 
        F.trunc("date", "MM")
    )
    .groupBy(
        "month_start", 
        "product_code", 
        "customer_code"
    )
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )
    .withColumnRenamed("month_start", "date")
)

display(df_monthly_recalc.orderBy(df_monthly_recalc.date.desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert operation (fact_orders)

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("parent_gold").merge(
    source = df_monthly_recalc.alias("child_gold"), 
    condition = "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.silver.staging_orders;