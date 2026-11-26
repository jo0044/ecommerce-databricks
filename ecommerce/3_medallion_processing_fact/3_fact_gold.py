# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df = spark.table(f"{catalog_name}.silver.slv_order_items")
display(df)

# COMMAND ----------

#add gross_amount(unit_price * quantity)
df1 = df.withColumn("gross_amount", F.col('unit_price')*F.col('quantity'))
df1.show()

# COMMAND ----------

#add discount_amount (gross * discount_pct/100)
df1 = df1.withColumn("discount_amount", F.col('gross_amount') * F.col('discount_pct')/100)
df1.show()

# COMMAND ----------

df1 = df1.withColumn('discount_amount', F.ceil(F.col('discount_amount')))

# COMMAND ----------

df1.show(3)

# COMMAND ----------

# add sale_amount = gross_amount - (discount_amount + tax_amount)
df1 = df1.withColumn("sale_amount", F.col('gross_amount')+(F.col('discount_amount')+F.col('tax_amount')))
df1.show(5)

# COMMAND ----------

# add date id
df_gold = df1.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key
# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df_gold = df_gold.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)
df_gold.limit(5).display()    

# COMMAND ----------

#change everything to rupees
fx_rates = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}

rates = [(k, float(v)) for k, v in fx_rates.items()]
rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()

# COMMAND ----------

df = (
    df_gold
    .join(
        rates_df,
        rates_df.currency == F.upper(F.trim(F.col("unit_price_currency"))),
        "left"
    )
    .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))#add a sale amount in inr column
    .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
)

# COMMAND ----------

df.show(5)

# COMMAND ----------

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)

# COMMAND ----------

display(orders_gold_df)

# COMMAND ----------

orders_gold_df.write.format("delta").mode("overwrite").option("mergeSchema",True).saveAsTable(f"{catalog_name}.gold.gld_fact_order_items")

# COMMAND ----------

