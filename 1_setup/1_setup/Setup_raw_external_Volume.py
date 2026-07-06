# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC USE CATALOG ecommerce;
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS raw.raw_landing
# MAGIC     LOCATION 'abfss://ecommerce-raw-data@stgsvadlsdevci001.dfs.core.windows.net/'
# MAGIC     COMMENT 'landing zone for raw data'

# COMMAND ----------

