# Databricks notebook source
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")

catalog = dbutils.widgets.get("catalog")

print(f"catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define start and end dates

# COMMAND ----------

# start date and end date
start_date = "2024-01-01"
end_date = "2025-12-01"

# COMMAND ----------

# Generate one row per month start between start_date and end_date
df = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 month
            )
        ) AS month_start_date
    """)
)

# Add useful analytics columns
df = (
    df
    .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
    .withColumn("year", F.year("month_start_date"))
    .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
    .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of DataFrame

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save in gold table

# COMMAND ----------

(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{gold_schema}.dim_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check of gold table

# COMMAND ----------

query = f"SELECT * FROM {catalog}.{gold_schema}.dim_date LIMIT 10;"

df_check = spark.sql(query)

display(df_check)