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

df_bronze = (
    spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove duplicate data

# COMMAND ----------

df_duplicates = (
    df_bronze
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

display(df_duplicates)

# COMMAND ----------

print("Rows before dropping duplicates: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(["customer_id"])
print("Rows after dropping duplicates: ", df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling leading and trailing spaces in string values

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn("customer_name", F.trim(F.col("customer_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spelling errors in city names

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

city_mapping = {
    "Bengaluruu" : "Bengaluru",
    "Bengalore" : "Bengaluru",

    "Hyderabadd" : "Hyderabad",
    "Hyderbad" : "Hyderabad",

    "NewDelhi" : "New Delhi",
    "NewDheli" : "New Delhi",
    "NewDelhee" : "New Delhi"  
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset = ["city"])
    .withColumn(
        "city", 
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

df_silver.select("city").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Title case fix in customer names

# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "customer_name", 
        F.when(F.col("customer_name").isNull(), None)
         .otherwise(F.initcap("customer_name"))
    )
)

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling NULL in city 

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customers_names = ["Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane"]
df_silver.filter(F.col("customer_name").isin(null_customers_names)).show(truncate=False)

# COMMAND ----------

# Simulation of city corrections confirmed by the business team.

customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix_null = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix_null)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix_null, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")
    )
    .drop("fixed_city")
)

display(df_silver.orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change data type of customer_id

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))

print(df_silver.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transforming DF to match the format of `dim_customers` table in the gold layer

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "customer", 
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in silver table 

# COMMAND ----------

(
    df_silver
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

# COMMAND ----------

# MAGIC %md
# MAGIC