# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

## creating three dataframes to perfrom the join operation

df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

## Create views - in databricks views are work as tables.

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")


# COMMAND ----------

display(spark.sql("select * from v_products limit(5)"))

# COMMAND ----------

display(spark.sql("select * from v_brands limit(5)"))

# COMMAND ----------

display(spark.sql("select * from v_category limit(5)"))

# COMMAND ----------

# Make sure we're on the right catalog 
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- Build brands x category mapping and write the gold table 
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS 
# MAGIC
# MAGIC WITH brands_category AS (
# MAGIC     SELECT 
# MAGIC         b.brand_name,
# MAGIC         b.brand_code,
# MAGIC         c.category_name,
# MAGIC         c.category_code
# MAGIC     FROM v_brands b 
# MAGIC     JOIN v_category c ON b.category_code = c.category_code
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.product_id,
# MAGIC     p.sku,
# MAGIC     p.category_code,
# MAGIC     coalesce(bc.category_name,"Not Available") AS category_name,
# MAGIC     p.brand_code,
# MAGIC     coalesce(bc.brand_name, "Not Available") AS brand_name,
# MAGIC     p.color,
# MAGIC     p.size,
# MAGIC     p.material,
# MAGIC     p.weight_grams,
# MAGIC     p.length_cm,
# MAGIC     p.width_cm,
# MAGIC     p.height_cm,
# MAGIC     p.rating_count,
# MAGIC     p._source_file,
# MAGIC     p.ingested_at
# MAGIC FROM v_products p 
# MAGIC LEFT JOIN brands_category bc ON p.brand_code = bc.brand_code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers 

# COMMAND ----------

#India states 

india_region = {
    "MH" : "West", "GJ": "West", "RJ": "West",
    "KA" : "South", "TN" : "South", "TS": "South", "AP": "South", "KL":"South",
    "UP": "North", "WB":"North", "DL": "North"
}

# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

#Combine into a master dictionary 
country_state_map = {
    "India": india_region,
    "Australia" : australia_region,
    "United Kingdom" : uk_region,
    "United States" : us_region,
    "United Arab Emirated" : uae_region,
    "Singapore" : singapore_region,
    "Canada" : canada_region
}

# COMMAND ----------

country_state_map

# COMMAND ----------

# 1 Flatten country_state_map into a list of Rows 

rows = []
for country, states in country_state_map.items():
    for state, region in states.items():
        rows.append(Row(country=country, state=state, region=region))

rows[:10]

# COMMAND ----------

# 2 Create mapping DataFrame

df_region_mapping = spark.createDataFrame(rows)

#show mapping 
df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_customers')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=['country','state'], how='left')
df_gold = df_gold.fillna({'region':'other'})
display(df_gold.limit(5))

# COMMAND ----------

desired_columns_order  = ['customer_id','phone','country_code','country','state','region','_source_file','ingested_at']

df_gold = df_gold.select(desired_columns_order)
display(df_gold.limit(5))

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Date/ Calender

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')
display(df_silver.limit(5))

# COMMAND ----------

df_silver.columns

# COMMAND ----------

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

## Add month name (e.g. 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# Add is_weekend column 
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin("Saturday","Sunday"),1).otherwise(0)

)

display(df_gold.limit(5))

# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", '_source_file','ingested_at']


# COMMAND ----------

df_gold = df_gold.select(desired_columns_order)
display(df_gold)

# COMMAND ----------

# write table to gold layer
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")