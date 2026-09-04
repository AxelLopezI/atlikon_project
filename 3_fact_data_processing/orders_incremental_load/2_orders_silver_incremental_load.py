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
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the tables

# COMMAND ----------

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from orders bronze staging table
# MAGIC

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.staging_{data_source};")

display(df_orders.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep only rows where order_qty is present

# COMMAND ----------

df_orders = (
    df_orders
    .filter(F.col("order_qty").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean customer_id: keep numeric, else set to 999999

# COMMAND ----------

df_orders = (
    df_orders
    .withColumn(
        "customer_id",
        F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
         .otherwise("999999")
         .cast("string")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove weekday name from the date text 
# MAGIC "Tuesday, July 01, 2025" → "July 01, 2025"

# COMMAND ----------

df_orders = (
    df_orders
    .withColumn(
        "order_placement_date",
        F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse order_placement_date using multiple possible formats

# COMMAND ----------

df_orders = (
    df_orders
    .withColumn(
        "order_placement_date",
        F.coalesce(
            F.try_to_date("order_placement_date", "yyyy/MM/dd"),
            F.try_to_date("order_placement_date", "dd-MM-yyyy"),
            F.try_to_date("order_placement_date", "dd/MM/yyyy"),
            F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
        )
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop duplicates

# COMMAND ----------

df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast product_id to string

# COMMAND ----------

df_orders = df_orders.withColumn('product_id', F.col('product_id').cast('string'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with products

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = (
    df_orders
    .join(
        df_products, 
        on="product_id", 
        how="inner"
    )
    .select(df_orders["*"], df_products["product_code"])
)

display(df_joined.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert operation (no staging table)

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    (
        df_joined
        .write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
else:
    silver_delta = DeltaTable.forName(spark, silver_table)

    silver_delta.alias("silver").merge(
        source = df_joined.alias("bronze"), 
        condition = "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging table to process just the arrived incremental data

# COMMAND ----------

(
    df_joined.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")
)