# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleansing and Transformation
# MAGIC
# MAGIC #### Order Items

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create Widgets 

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stgsvadlsdevci001", "Storage Account Name")
dbutils.widgets.text("container_name", "ecommerce-raw-data","Cotainer Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Stream Bronze Table in DataFrame

# COMMAND ----------

df = spark.readStream \
.format("delta") \
.table(f"{catalog_name}.bronze.brz_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Transformations and Cleaning

# COMMAND ----------

df = df.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' - 2 cast to Integer 

df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Transformation : Remove any '$' or other symbols from unti_price, keep only numeric 

df = df.withColumn(
    "unit_price",
    F.regexp_replace(F.col("unit_price"), "[$]", "")
)

# Transformation : Remove '%' from discount_pct and cast to double

df = df.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

#Transformation: coupon code processing (convert to lower)

df = df.withColumn("coupon_code", F.lower(F.trim("coupon_code"))
)

## Transformation : channel processing

df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
     .when(F.col("channel") == "app", "Mobile")
     .otherwise(F.col("channel")),
)

## Transformation : Add Processed time

df = df.withColumn(
    "processed_time",F.current_timestamp(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Silver Table 

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Checkpoint for Silver Tables 

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_items/"
print(silver_checkpoint_path)

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchId):
    table_name = f"{catalog_name}.silver.slv_order_items"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("silver_table").merge(
            microBatchDF.alias("batch_table"),
            "silver_table.order_id = batch_table.order_id AND silver_table.item_seq = batch_table.item_seq",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()    

    

# COMMAND ----------

# This line is running a Structured Streaming job that:
# - Reads incremental data from Bronze (df).
# - For each batch → applies upsert_to_silver (update if exists, insert if not).
# - Writes into a Silver Delta table with schema evolution enabled.
# - Uses checkpointing for recovery.
# - Runs in batch-like mode (once or availableNow), not continuous streaming.

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) FROM {catalog_name}.silver.slv_order_items").show()

# COMMAND ----------

spark.sql(f"SELECT max(order_ts) FROM {catalog_name}.silver.slv_order_items").show()

# COMMAND ----------

spark.sql(f"SELECT min(order_ts) FROM {catalog_name}.silver.slv_order_items").show()

# COMMAND ----------

