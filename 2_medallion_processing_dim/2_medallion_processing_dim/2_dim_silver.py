# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleaning and Transformation for Dimension Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType 

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table :- Brands

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

## removing the extra spaces in brand_name column 

df_silver = df_bronze.withColumn("brand_name",F.trim(F.col("brand_name")))
df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))
display(df_silver)

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

# anomalies dictionary
anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

#pySpark repalce is easy 

df_silver = df_silver.replace(to_replace=anomalies,subset=["category_code"])
display(df_silver)

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

# write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_brands)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table :- Category

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_category')
df_bronze.show(10)

# COMMAND ----------

df_duplicates = df_bronze.groupBy("category_code").count().filter("count > 1")
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["category_code"])
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
df_silver.show(10)

# COMMAND ----------

# write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table :- Products

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")

#get row and column count 
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# print results 
print(f"Row Count: {row_count}")
print(f"Column Count: {column_count}")

# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Check weight_grams contains 'g'

# COMMAND ----------

df_bronze.select('weight_grams').show(5)

# COMMAND ----------

df_silver = df_bronze.withColumn("weight_grams", F.regexp_replace(F.col("weight_grams"), r'g', '').cast(IntegerType()))
df_silver.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Check length_cm (comma instead of dot)

# COMMAND ----------

df_silver.select("length_cm").show(5, truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("length_cm", F.regexp_replace(F.col("length_cm"), r',','.').cast(FloatType()))
df_silver.select("length_cm").show(5, truncate=False) 


# COMMAND ----------

# MAGIC %md
# MAGIC category_code and brand_code are in lower case. we need to make it all upper case

# COMMAND ----------

df_silver.select("category_code", "brand_code").show(5)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code"))) \
                     .withColumn("brand_code", F.upper(F.col("brand_code")))
display(df_silver.limit(5))                     

# COMMAND ----------

# MAGIC %md
# MAGIC Spelling mistakes in material column

# COMMAND ----------

df_silver.select("material").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
            "material",
                F.when(F.col("material") == "Coton", "Cotton")
                 .when(F.col("material") == "Alumium" , "Aluminum")
                 .when(F.col("material") == "Ruber", "Rubber")
                 .otherwise(F.col("material")))

df_silver.select("material").distinct().show()                 

# COMMAND ----------

# MAGIC %md
# MAGIC Negative values in rating_count

# COMMAND ----------

df_silver.filter(F.col('rating_count')<0).select("rating_count").show(5)

# COMMAND ----------

#convert negative rating_count to positive
df_silver = df_silver.withColumn("rating_count", F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count"))).otherwise(F.lit(0)))

# COMMAND ----------

## check final cleanded data 

df_silver.select("weight_grams",
                 "length_cm",
                 "category_code",
                 "brand_code",
                 "material",
                 "rating_count").show(15, truncate=False)

# COMMAND ----------

# write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_products)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table - Customers

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")

#get row_count and column count 

row_count, column_count = df_bronze.count(), len(df_bronze.columns)

#print the result 
print(f"Row count: {row_count}")
print(f"Column Count:{column_count}")

display(df_bronze.limit(5)) 

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in customer_id column

# COMMAND ----------

null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

## There are 300 null values in customer_id column. Display some of those

df_bronze.filter(F.col("customer_id").isNull()).show(5)

# COMMAND ----------

# drop rows where 'customer_id' is null

df_silver = df_bronze.dropna(subset=["customer_id"])

#get row count 
row_count = df_silver.count()
print(f"Row count after dropping null values: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in phone column

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

null_count = df_silver.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}")

# COMMAND ----------

## fill the values with "Not Avaialble"
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# sanity check ( if anu null still exits)
df_silver.filter(F.col("phone").isNull()).show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table :- Calender/Date

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_date")

#get row & column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

#print the result 
print(f"Row count: {row_count}")
print(f"Column count:{column_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicates 

# COMMAND ----------

# Find duplicates rows in the Dataframe

duplicates = df_bronze.groupBy('date').count().filter("count>1")

# show the duplicate rows 
print("Total duplicated rows:", duplicates.count())
display(duplicates)

# COMMAND ----------

#Remove_duplicates
df_silver = df_bronze.dropDuplicates(['date'])

#Get row count
row_count = df_silver.count()

print("Rows After removing Duplicates:", row_count)


# COMMAND ----------

# MAGIC %md
# MAGIC day_name normalize casing

# COMMAND ----------

# Capitalize first letter of each word in day_name

df_silver = df_silver.withColumn("day_name",F.initcap(F.col("day_name")))

df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert negative week_of_year to positive

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))

df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Enhance quarter and week_of_year column

# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"),F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("", F.concat(F.lit("W"), F.col("week_of_year"),F.lit("-"), F.col("year"))))

df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("week_of_year", "week")


# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")

# COMMAND ----------

