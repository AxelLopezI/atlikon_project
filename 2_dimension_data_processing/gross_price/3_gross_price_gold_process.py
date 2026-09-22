# Databricks notebook source
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../../1_setup/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from gross price silver table 

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final columns

# COMMAND ----------

df_gold = df_silver.select("product_code", "month", "gross_price")

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
# MAGIC ## Get the price for each product_code (aggregated by year)

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")

display(df_gold_price.limit(10))

# COMMAND ----------

df_gold_price = (
    df_gold_price
    .withColumn(
        "year", 
        F.year("month")
    )
    # 0 = non-zero price, 1 = zero price
    .withColumn(
        "is_zero", 
        F.when(F.col("gross_price") == 0, 1)
         .otherwise(0)
    )
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(
        F.col("is_zero"), 
        F.col("month").desc()
    )
)

df_gold_latest_price = (
    df_gold_price
    .withColumn(
        "rnk", 
        F.row_number().over(w)
    )
    .filter(F.col("rnk") == 1)
)

display(df_gold_latest_price)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final columns

# COMMAND ----------

df_gold_latest_price = (
    df_gold_latest_price
    .select(
        "product_code", 
        "year", 
        "gross_price"
    )
    .withColumnRenamed(
        "gross_price", 
        "price_inr"
    )
    .select(
        "product_code", 
        "price_inr", 
        "year"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change year to string

# COMMAND ----------

df_gold_latest_price = (
    df_gold_latest_price
    .withColumn(
        "year", 
        F.col("year").cast("string")
    )
)

display(df_gold_latest_price.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert operation with final dim table

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")

delta_table.alias("target").merge(
    source = df_gold_latest_price.alias("source"),
    condition = "target.product_code = source.product_code"
).whenMatchedUpdate(
    set = {
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values = {
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()