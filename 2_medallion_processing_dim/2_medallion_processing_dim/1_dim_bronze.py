# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brand

# COMMAND ----------

# Define Schema for the data file 
brand_schema = StructType([
    StructField('brand_code', StringType(), False),
    StructField('brand_name', StringType(), True),
    StructField('category_code', StringType(), True),
])

# COMMAND ----------

raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/brands/*.csv"

df = spark.read.option('header', "True").option('delimiter', ',').csv(raw_data_path)

# add metadata column 

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp()) 

display(df.limit(5))

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Category

# COMMAND ----------

# Define Schema for the data file 
category_schema = StructType([
    StructField('category_code', StringType(), False),
    StructField('category_name', StringType(), True),
])

# COMMAND ----------

raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/category/*.csv"

df = spark.read.option('header', "True").option('delimiter', ',').csv(raw_data_path)

# add metadata column 

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp()) 

display(df.limit(5))

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cusotmers

# COMMAND ----------

# Define Schema for the data file 
customer_schema = StructType([
    StructField('customer_id', StringType(), False),
    StructField('phone', StringType(), True),
    StructField('country_code',StringType(), True),
    StructField('country', StringType(), True),
    StructField('state', StringType(), True)
])

# COMMAND ----------

raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/customers/*.csv"

df = spark.read.option('header', "True").option('delimiter', ',').csv(raw_data_path)

# add metadata column 

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp()) 

display(df.limit(5))

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date

# COMMAND ----------

#Define schema for the data file
date_schema = StructType([
    StructField("date", DateType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year",IntegerType(), True),
])

raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/date/*.csv"

df = spark.read.option('header', "True").option('delimiter', ',').csv(raw_data_path)

# add metadata column 

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp()) 

display(df.limit(5))

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product

# COMMAND ----------

#Define schema for the data file
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm",StringType(), True),
    StructField("width_cm", StringType(), True),
    StructField("height_cm", StringType(), True),
    StructField("rating_count", IntegerType(), True),
])

catalog_name = 'ecommerce'


raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/products/*.csv"


df = spark.read.option('header', "True").option('delimiter', ',').csv(raw_data_path)

# add metadata column 

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp()) 

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")

# COMMAND ----------

