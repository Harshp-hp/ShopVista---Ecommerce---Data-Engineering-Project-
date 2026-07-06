# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleansing and Transformation
# MAGIC
# MAGIC #### Order Returns

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.functions import col

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
# MAGIC ### Stream Bronze Table in Data Frame

# COMMAND ----------

df = spark.readStream \
.format("delta") \
.table(f"{catalog_name}.bronze.brz_order_returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Transformaation and Cleaning 

# COMMAND ----------

## Dropping the duplicates by using "order_id", "order_dt", & "return_ts"

df = df.dropDuplicates(["order_id", "order_dt", "return_ts"])

# Transformation : Convert 'order_dt' column to DateType
df = df.withColumn("order_dt", col("order_dt").cast("Date"))

# Transformation : Convert "return_ts" column to Timestamp datatype
df = df.withColumn("return_ts", col("return_ts").cast("Timestamp"))

# Transformation : Convert "reason" to upper case and trim whitespaces 
df = df.withColumn("reason", F.upper(F.trim("reason")))

# Transformation : Add Processed Time 
df = df.withColumn(
    "processed_time", F.current_timestamp(),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a checkpoint for Silver table

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_returns/"
print(silver_checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Run silver order returns stream
def upsert_to_silver(microBatchDF, batchId):
    table_name = f"{catalog_name}.silver.slv_order_returns"
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
            "silver_table.order_id = batch_table.order_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


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

spark.sql(f"SELECT COUNT(*) FROM {catalog_name}.silver.slv_order_returns").show()

# COMMAND ----------

