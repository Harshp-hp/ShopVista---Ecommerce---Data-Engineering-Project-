# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce 
# MAGIC MANAGED LOCATION 'abfss://uc-data@stgsvadlsdevci001.dfs.core.windows.net/ecommerce-catalog'
# MAGIC COMMENT 'Ecommerce Project catalog in Central India, backed by external location'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW DATABASES from ecommerce

# COMMAND ----------

