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
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from gross price bronze table 

# COMMAND ----------

df_bronze = (
    spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
)

display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize `month` field

# COMMAND ----------

df_bronze.select('month').distinct().show()

# COMMAND ----------

# Parse 'month' from multiple possible formats
date_formats = ["yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = (
    df_bronze
    .withColumn(
        "month",
        F.coalesce(
            F.try_to_date(F.col("month"), "yyyy/MM/dd"),
            F.try_to_date(F.col("month"), "dd/MM/yyyy"),
            F.try_to_date(F.col("month"), "yyyy-MM-dd"),
            F.try_to_date(F.col("month"), "dd-MM-yyyy")
        )
    )
)

# COMMAND ----------

df_silver.select('month').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling 'gross_price'

# COMMAND ----------

# Converting only valid numeric values to double
# Fixing negative prices by making them positive
# Replacing all non-numeric values with 0

df_silver = (
    df_silver
    .withColumn(
        "gross_price",
        F.when(
            F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'), 
            F.when(F.col("gross_price").cast("double") < 0, -1 * F.col("gross_price").cast("double"))
             .otherwise(F.col("gross_price").cast("double"))
        ).otherwise(0)
    )
)

display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich silver dataset 
# MAGIC By performing an inner join with the products table to fetch the correct product_code for each product_id

# COMMAND ----------

df_products = spark.table("fmcg.silver.products") 
df_joined = (
    df_silver
    .join(
        df_products.select("product_id", "product_code"), 
        on="product_id", 
        how="inner"
    )
)
df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")

display(df_joined.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in silver table 

# COMMAND ----------

(
    df_joined
    .write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .option("mergeSchema", "true")
    .mode("overwrite") 
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of silver table

# COMMAND ----------

query = f"SELECT * FROM {catalog}.{silver_schema}.{data_source} LIMIT 10;"

df_check = spark.sql(query)

display(df_check.limit(10))