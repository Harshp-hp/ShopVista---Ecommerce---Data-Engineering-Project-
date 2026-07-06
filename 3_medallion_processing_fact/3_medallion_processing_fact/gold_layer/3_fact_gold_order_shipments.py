# Databricks notebook source
# MAGIC %md
# MAGIC ### From Silver To Gold: Aggregation and KPI Tables

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stgsvadlsdevci001", "Storage Account Name")
dbutils.widgets.text("container_name", "ecommerce-raw-data", "Container Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

# DBTITLE 1,Preview silver CDF stream
df = spark.readStream \
.format("delta") \
.option("readChangeFeed", "true") \
.table(f"{catalog_name}.silver.slv_order_shipments")


# COMMAND ----------

df_union = df.filter("_change_type IN ('insert', 'update_postimage')")

# COMMAND ----------

df_union = df_union.withColumn("order_day_name", F.dayname("order_dt"))

# Add is_weekend column 
df_union = df_union.withColumn(
    "is_weekend",
    F.when(F.col("order_day_name").isin("Saturday","Sunday"),1).otherwise(0)
)

domestic_carriers = ["ECOMEXPRESS", "DELHIVERY", "XPRESSBEES", "BLUEDART"]

df_union = df_union.withColumn(
    "carrier_group",
    F.when(F.col("carrier").isin(domestic_carriers), "Domestic").otherwise("International")
)

df_union = df_union.withColumn("date_id", F.date_format(F.col("order_dt"), "yyyyMMdd").cast("int"))

# COMMAND ----------

orders_gold_df = df_union.select(
    F.col("date_id"),
    F.col("shipment_id"),
    F.col("order_dt").alias("order_date"),
    F.col("order_id").alias("transaction_id"),
    F.col("carrier"),
    F.col("carrier_group"),
    F.col("order_day_name"),
    F.col("is_weekend"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Gold Table

# COMMAND ----------

# DBTITLE 1,Cell 11
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_shipments/"
print(gold_checkpoint_path)

def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_fact_order_shipments"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.transaction_id = batch_table.transaction_id and gold_table.shipment_id = batch_table.shipment_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

orders_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) FROM {catalog_name}.gold.gld_fact_order_shipments").show()

# COMMAND ----------

