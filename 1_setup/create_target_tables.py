# Databricks notebook source
# MAGIC %md
# MAGIC ## Creates the empty gold-layer target tables that the dimension and fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")

catalog = dbutils.widgets.get("catalog")

print(f"catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customers

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customers (
  customer_code STRING,
  customer      STRING,
  market        STRING,
  platform      STRING,
  channel       STRING
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_products

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_products (
  product_code STRING,
  division     STRING,
  category     STRING,
  product      STRING,
  variant      STRING
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_gross_price

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_gross_price (
  product_code STRING,
  price_inr    DOUBLE,
  year         STRING
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_orders

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_orders (
  date           DATE,
  customer_code  STRING,
  product_code   STRING,
  sold_quantity  INT
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

for table in ["dim_customers", "dim_products", "dim_gross_price", "fact_orders"]:
    print(f"{catalog}.gold.{table}")
    spark.sql(f"DESCRIBE TABLE {catalog}.gold.{table}").show(truncate=False)