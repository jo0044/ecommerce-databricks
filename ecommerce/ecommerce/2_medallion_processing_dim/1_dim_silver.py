# Databricks notebook source
# MAGIC %md
# MAGIC ##  **BRANDS**

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType, DateType, TimestampType
from pyspark.sql import functions as F

# COMMAND ----------

catalog_name = "ecommerce"
df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")
df_bronze.show()

# COMMAND ----------

#remove extra trailing spaces from brand_code
df_silver = df_bronze.withColumn('brand_name', F.trim(F.col("brand_name"))) 
df_silver.show()

# COMMAND ----------

#now lets just say the brand_code should be alpha numeric meaning it should only have alphabets and numbers
#so we can use the regexp_replace function to replace any non alphanumeric characters with an empty string
from pyspark.sql.functions import regexp_replace
df_silver = df_silver.withColumn("brand_code",F.regexp_replace("brand_code","[^a-zA-Z0-9]",""))
df_silver.show()

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

df_silver = df_silver.replace("BKS","BOOKS", subset='category_code')

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

fix = {"GRCY":"GROCERY", "TOYS":"TOY"}
df_silver = df_silver.replace(fix, subset="category_code")
df_silver.select("category_code").distinct().show()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CATEGORIES**

# COMMAND ----------

catalog_name = "ecommerce"
df_bronze = spark.table(f"{catalog_name}.bronze.brz_categories")
df_bronze.show(truncate=False)


# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType, DateType, TimestampType
from pyspark.sql import functions as F

# COMMAND ----------

dup = df_bronze.groupBy('category_code').count().filter('count > 1')
dup.show()

# COMMAND ----------

df_bronze = df_bronze.dropDuplicates(['category_code'])

# COMMAND ----------

dup = df_bronze.groupBy('category_code').count().filter('count > 1')
dup.show()

# COMMAND ----------

df_silver = df_bronze.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.silver.slv_categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CUSTOMERS**

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")
df_bronze.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce.bronze.brz_customers where customer_id is null

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

df_silver = df_bronze.na.drop(how="all")
df_silver.count()

# COMMAND ----------

df_silver = df_silver.na.drop(subset=['customer_id'])
df_silver.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce.bronze.brz_customers where country is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce.bronze.brz_customers where phone is null

# COMMAND ----------

df_silver = df_bronze.na.fill("Not available", subset="phone")

# COMMAND ----------

df_silver.filter(df_silver['phone'].isNull()).count()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce.silver.slv_customers where phone is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DATE**

# COMMAND ----------

catalog_name="ecommerce"
df_bronze = spark.table(f"{catalog_name}.bronze.brz_date")
df_bronze.show()

# COMMAND ----------

from pyspark.sql.functions import to_date
# Convert the string column to a date type
df_silver = df_bronze.withColumn("date", to_date(df_bronze["date"], "dd-MM-yyyy"))

# COMMAND ----------

df_silver = df_silver.dropDuplicates(['date'])


# COMMAND ----------

from pyspark.sql import functions as F
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))
df_silver.show(5)

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col('week_of_year')))
df_silver.show()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType, DateType, TimestampType
from pyspark.sql import functions as F
df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver.show(3)

# COMMAND ----------

#rename columns
df_silver = df_silver.withColumnRenamed("week_of_year","week")


# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **PRODUCTS**

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")
df_bronze.show()

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

df_silver = df_bronze.na.drop(how="all")
df_silver.count()

# COMMAND ----------

df_silver = df_bronze.withColumn("weight_grams",F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType()))

# COMMAND ----------

df_silver = df_silver.withColumn("length_cm",F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType()))

# COMMAND ----------

df_silver.select('category_code','brand_code').show()

# COMMAND ----------

from pyspark.sql import functions as F
df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code"))). withColumn("brand_code", F.upper(F.col("brand_code")))

# COMMAND ----------

df_silver.select("category_code").show()

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct material from ecommerce.bronze.brz_products

# COMMAND ----------

#fix spelling mistakes in materials
fix = {
    "Coton":"Cotton",
    "Ruber":"Rubber",
    "Alumium":"Aluminium"
}
df_silver = df_silver.replace(fix, subset='material')


# COMMAND ----------

df_silver.select('material').distinct().show()

# COMMAND ----------

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))  # if null, replace with 0
)

# COMMAND ----------

# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

