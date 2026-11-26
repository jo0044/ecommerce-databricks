# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType, DateType, TimestampType
from pyspark.sql import functions as F

# COMMAND ----------

#create your data frame iwth brands.csv from the source_data/raw folder
catalog_name = "ecommerce"

brand_schema = StructType([
    StructField("brand_code", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True)
])

df = spark.read.schema(brand_schema).csv("/Volumes/ecommerce/source_data/raw/brands.csv", header=True)
df.show()


# COMMAND ----------

#add the metadata columns for audits
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())
display(df)

# COMMAND ----------

#write the dataframe into the bronze table
df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType, DateType, TimestampType
from pyspark.sql import functions as F
#create df for categories from categories.csv
category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])
df = spark.read.schema(category_schema).csv("/Volumes/ecommerce/source_data/raw/category.csv", header=True)
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())
display(df)


# COMMAND ----------

#write it into a delta table in bronze schema
catalog_name = "ecommerce"
df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.bronze.brz_categories")

# COMMAND ----------

#make a dataframe for the products csv
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm", StringType(), True),
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), True),
    StructField("ingest_timestamp", TimestampType(), True)
])
df = spark.read.schema(product_schema).csv("/Volumes/ecommerce/source_data/raw/products.csv",header=True)
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())
display(df)


# COMMAND ----------

#write it into a delta table in bronze schema
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_products")

# COMMAND ----------

#create df for customers.csv
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType, DateType, TimestampType
from pyspark.sql import functions as F
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])
df = spark.read.schema(customer_schema).csv("/Volumes/ecommerce/source_data/raw/customers.csv", header=True)
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

#write it into a delta table in bronze schema
catalog_name = "ecommerce"
df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.bronze.brz_customers")
display(df)


# COMMAND ----------

#date.csv into a df and then into az delta table in the bronze schema
date_schema = StructType([
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True)
])
df = spark.read.schema(date_schema).csv("/Volumes/ecommerce/source_data/raw/date.csv", header=True)
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())
#write this into a delta table on bronze schema
df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.bronze.brz_date")
display(df)


# COMMAND ----------

