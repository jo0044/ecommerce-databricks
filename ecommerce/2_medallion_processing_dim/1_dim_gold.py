# Databricks notebook source
# MAGIC %md
# MAGIC ## **PRODUCTS**

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

dfp = spark.table(f"{catalog_name}.silver.slv_products")
dfc = spark.table(f"{catalog_name}.silver.slv_categories")
dfb = spark.table(f"{catalog_name}.silver.slv_brands")
dfp.createOrReplaceTempView("prod")
dfc.createOrReplaceTempView("categ")
dfb.createOrReplaceTempView("brand")

# COMMAND ----------

dfp.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.*, c.category_name from prod p join categ c on c.category_code=p.category_code 

# COMMAND ----------

df_products = spark.sql("select p.product_id,p.sku,p.category_code,c.category_name,p.brand_code,p.color,p.size,p.material,p.weight_grams,p.length_cm,p.width_cm,p.height_cm,p.rating_count,p.file_name,p.ingest_timestamp,p._source_file,p.ingested_at from prod p join categ c on c.category_code=p.category_code ")
display(df_products)

# COMMAND ----------

display(df_products.columns)

# COMMAND ----------

df_products.createOrReplaceTempView("prodnew")

# COMMAND ----------

df_pb = spark.sql("select p.product_id, p.sku, p.category_code, p.category_name, p.brand_code, b.brand_name, p.color, p.size, p.material, p.weight_grams, p.length_cm, p.width_cm, p.height_cm, p.rating_count, p.file_name, p.ingest_timestamp, p._source_file, p.ingested_at from prodnew p join brand b on b.brand_code=p.brand_code")
display(df_pb)

# COMMAND ----------

df_pb.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CUSTOMERS**

# COMMAND ----------

dfcust = spark.table(f"{catalog_name}.silver.slv_customers")
dfcust.select("state").distinct().show()

# COMMAND ----------

#create a dictionay using python for regions based on states

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
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

#combine into a mater disctionary with the states and their countries 
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# COMMAND ----------

country_state_map

# COMMAND ----------

#okay so now put it into rows so you can 
rows = []
for country,states in country_state_map.items():
    for state,region in states.items():
        rows.append(Row(country=country, state=state, region=region))
print(rows)

# COMMAND ----------

rows[0:]

# COMMAND ----------

#now create a dataframe from the rows
df_rows = spark.createDataFrame(rows)
display(df_rows)

# COMMAND ----------

df_gold = dfcust.join(df_rows, on=['country','state'], how="left")
display(df_gold, truncate=False)

# COMMAND ----------

df_gold1 = df_gold.na.fill("other", subset='region')

# COMMAND ----------

display(df_gold1.columns)

# COMMAND ----------

df_gold1.select("region").distinct().show()

# COMMAND ----------

df_gold2 = df_gold1.select('customer_id','phone','country_code','country','state','region','_source_file','ingested_at')
display(df_gold2)

# COMMAND ----------

df_gold2 = df_gold2.replace("other","Other", subset='region')
df_gold2.select("region").distinct().show()

# COMMAND ----------

df_gold2.write.format("delta").mode("overwrite").option("mergeSchema",True).saveAsTable(f"{catalog_name}.gold.gld_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CALENDAR**

# COMMAND ----------

dfcal = spark.table(f"{catalog_name}.silver.slv_calendar")

# COMMAND ----------

df_gold = dfcal.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
)
display(df_gold)

# COMMAND ----------

display(df_gold.columns)

# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").option("mergeSchema",True).saveAsTable(f"{catalog_name}.gold.gld_calendar")

# COMMAND ----------

