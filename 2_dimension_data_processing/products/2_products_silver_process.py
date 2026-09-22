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
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(f"catalog: {catalog}, data_source: {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from products bronze table
# MAGIC

# COMMAND ----------

df_bronze = (
    spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
)

display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove duplicate data

# COMMAND ----------

print("Rows before dropping duplicates: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print("Rows after dropping duplicates: ", df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Title case fix

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "category", 
        F.when(F.col("category").isNull(), None)
         .otherwise(F.initcap("category"))
    )
)

df_silver.select('category').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix spelling mistake for `Protien`

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "product_name",
        F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")
    )
    .withColumn(
        "category",
        F.regexp_replace(F.col("category"), "(?i)Protien", "Protein")
    )
)

display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardizing customer attributes to match data model

# COMMAND ----------

# Add division column
df_silver = (
    df_silver
    .withColumn(
        "division",
        F.when(F.col("category") == "Energy Bars",        "Nutrition Bars")
         .when(F.col("category") == "Protein Bars",       "Nutrition Bars")
         .when(F.col("category") == "Granola & Cereals",  "Breakfast Foods")
         .when(F.col("category") == "Recovery Dairy",     "Dairy & Recovery")
         .when(F.col("category") == "Healthy Snacks",     "Healthy Snacks")
         .when(F.col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
         .otherwise("Other")
    )
)

# COMMAND ----------

# Add variant column
df_silver = (
    df_silver
    .withColumn(
        "variant", 
        F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
    )
)

# COMMAND ----------

# Create column 'product_code'  
# Invalid product_ids are replaced with a fallback value to avoid losing fact records and ensure downstream joins remain consistent

df_silver = (
    df_silver
    # Generate deterministic product_code from product_name
    .withColumn(
        "product_code",
        F.sha2(F.col("product_name").cast("string"), 256)
    )
    # Clean product_id: keep only numeric IDs, else set to 999999
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").cast("string").rlike("^[0-9]+$"),
            F.col("product_id").cast("string")
        ).otherwise(F.lit(999999).cast("string"))
    )
    # Rename product_name → product
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final columns

# COMMAND ----------

df_silver = (
    df_silver
    .select(
        "product_code", 
        "division", 
        "category", 
        "product", 
        "variant", 
        "product_id", 
        "read_timestamp", 
        "file_name", 
        "file_size"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of DataFrame

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in silver table

# COMMAND ----------

(
    df_silver.write
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