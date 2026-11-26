# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df = spark.table(f"{catalog_name}.bronze.brz_order_items")
display(df)

# COMMAND ----------

df.groupBy("order_id").count().filter(F.col("count")>1).show()

# COMMAND ----------

df = df.dropDuplicates(["order_id", "item_seq"])

# COMMAND ----------

df.groupBy("quantity").count().filter(F.col("count")>1).show()

# COMMAND ----------

df.select("quantity").distinct().show()

# COMMAND ----------

#you can not use replace to change Two to 2 because they are different datatypes
df1 = df.withColumn("quantity", F.when(F.col('quantity') == "Two", 2).otherwise(F.col('quantity')).cast('int'))
df1.show()

# COMMAND ----------

#or if you want to use replace do this. either way both have to be cast into int datatype
df2 = df.replace("Two", "2", subset='quantity').withColumn('quantity', F.col('quantity').cast('int'))
df2.show()

# COMMAND ----------

df1.createOrReplaceTempView("df1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df1 where unit_price like '%$%'

# COMMAND ----------

#remove $ from the unit_price. you can not use replace because there is something after the $. so use F.regexp_replace
df_silver = df1.withColumn("unit_price",F.regexp_replace("unit_price", "[$]", "").cast("double"))
df_silver.show()

# COMMAND ----------

df_silver = df_silver.withColumn("discount_pct",F.regexp_replace("discount_pct", "%", "").cast("double"))
df_silver.show()

# COMMAND ----------

#change coupon_code to lower
df_silver = df_silver.withColumn("coupon_code", F.lower(F.trim(F.col("coupon_code"))))


# COMMAND ----------

df_silver.createOrReplaceTempView("slv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from slv where channel='' or channel not in ('app','web')

# COMMAND ----------

#change app to Mobile and web to website
fix = {"app":"Mobile","web":"Website"}
df_silver = df_silver.replace(fix, subset='channel')
df_silver.show()

# COMMAND ----------

# Transformation: datatype conversions
# 1) Convert dt (string → date)
df_silver = df_silver.withColumn("dt", F.to_date("dt", "yyyy-MM-dd"))
# 2) Convert order_ts (string → timestamp)
df_silver = df_silver.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")      # fallback for 01-08-2025 22:53
    )
)


# 3) Convert item_seq (string → integer)
df_silver = df_silver.withColumn(
    "item_seq",
    F.col("item_seq").cast("int")
)

# 4) Convert tax_amount (string → double, strip non-numeric characters)
df_silver = df_silver.withColumn(
    "tax_amount",
    F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
)


#Transformation : Add processed time 
df_silver = df_silver.withColumn(
    "processed_time", F.current_timestamp()
)
display(df_silver)

# COMMAND ----------

#now that the schema is correct and been cast into the desired datatypes, write into the silver schema
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", True).saveAsTable(f"{catalog_name}.silver.slv_order_items")

# COMMAND ----------

