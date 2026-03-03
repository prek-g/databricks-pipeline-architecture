# Databricks notebook source
# MAGIC %md
# MAGIC ### Dependencies and variables

# COMMAND ----------

dbutils.widgets.text("src", "")

# COMMAND ----------

src = dbutils.widgets.get("src")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paths:

# COMMAND ----------

input_path = f"/Volumes/workspace/iron/raw_files/raw_json/finance_project/{src}_data/"
checkpoint = f"/Volumes/workspace/bronze/bronze_volume/finance_project/{src}_data/checkpoint"
delta_path = f"/Volumes/workspace/bronze/bronze_volume/finance_project/{src}_data/query"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring Schemas

# COMMAND ----------

users_schema = StructType([
    StructField("customer", StructType([
        StructField("id", StringType(), True),
        StructField("personal", StructType([
            StructField("address", StringType(), True),
            StructField("birth_month", StringType(), True),
            StructField("birth_year", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("current_age", StringType(), True),
            StructField("retirement_age", StringType(), True)
        ]), True),
        StructField("financial", StructType([
            StructField("credit_score", StringType(), True),
            StructField("num_credit_cards", StringType(), True),
            StructField("total_debt", StringType(), True),
            StructField("yearly_income", StringType(), True),
            StructField("per_capita_income", StringType(), True)
        ]), True),
        StructField("location", StructType([
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ]), True)
    ]), True)
])

cards_schema = StructType([
    StructField("card", StructType([
        StructField("id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("details", StructType([
            StructField("card_brand", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("card_number", StringType(), True),
            StructField("expires", StringType(), True),
            StructField("cvv", StringType(), True),
            StructField("has_chip", StringType(), True)
        ]), True),
        StructField("history", StructType([
            StructField("num_cards_issued", StringType(), True),
            StructField("credit_limit", StringType(), True),
            StructField("acct_open_date", StringType(), True),
            StructField("year_pin_last_changed", StringType(), True)
        ]), True),
        StructField("security", StructType([
            StructField("card_on_dark_web", StringType(), True)
        ]), True)
    ]), True)
])

transactions_schema = StructType([
    StructField("transaction", StructType([
        StructField("id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("client", StructType([
            StructField("client_id", StringType(), True),
            StructField("card_id", StringType(), True),
            StructField("use_chip", StringType(), True)
        ]), True),
        StructField("merchant", StructType([
            StructField("merchant_id", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("mcc", StringType(), True)
        ]), True),
        StructField("errors", StringType(), True)  # guaranteed column
    ]), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map source to schema

# COMMAND ----------

schema_map = {
    "users": users_schema,
    "cards": cards_schema,
    "transactions": transactions_schema
}

# COMMAND ----------

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema_map[src])
    .option("cloudFiles.schemaLocation", checkpoint)  # same checkpoint
    .option("multiline", True)
    .load(input_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flattening and casting columns

# COMMAND ----------

if src == "users":
    df_flattened = df_stream.select(
        col("customer.id").cast(IntegerType()).alias("customer_id"),
        col("customer.personal.address").alias("address"),
        col("customer.personal.birth_month").alias("birth_month"),
        col("customer.personal.birth_year").cast(IntegerType()).alias("birth_year"),
        col("customer.personal.gender").alias("gender"),
        col("customer.personal.current_age").cast(IntegerType()).alias("current_age"),
        col("customer.personal.retirement_age").cast(IntegerType()).alias("retirement_age"),
        col("customer.financial.credit_score").cast(IntegerType()).alias("credit_score"),
        col("customer.financial.num_credit_cards").cast(IntegerType()).alias("num_credit_cards"),
        regexp_replace(col("customer.financial.total_debt").cast("string"), "[$,]", "").cast(DoubleType()).alias("total_debt"),
        regexp_replace(col("customer.financial.yearly_income").cast("string"), "[$,]", "").cast(DoubleType()).alias("yearly_income"),
        regexp_replace(col("customer.financial.per_capita_income").cast("string"), "[$,]", "").cast(DoubleType()).alias("per_capita_income"),
        regexp_replace(col("customer.location.latitude").cast("string"), "[$,]", "").cast(DoubleType()).alias("latitude"),
        regexp_replace(col("customer.location.longitude").cast("string"), "[$,]", "").cast(DoubleType()).alias("longitude")
    )

elif src == "cards":
    df_flattened = df_stream.select(
        col("card.id").cast(IntegerType()).alias("card_id"),
        col("card.client_id").cast(IntegerType()).alias("client_id"),
        col("card.details.card_brand").alias("card_brand"),
        col("card.details.card_type").alias("card_type"),
        col("card.details.card_number").cast(LongType()).alias("card_number"),
        col("card.details.expires").alias("expires"),
        col("card.details.cvv").cast(IntegerType()).alias("cvv"),
        col("card.details.has_chip").cast(BooleanType()).alias("has_chip"),
        col("card.history.num_cards_issued").cast(IntegerType()).alias("num_cards_issued"),
        regexp_replace(col("card.history.credit_limit").cast("string"), "[$,%]", "").cast(DoubleType()).alias("credit_limit"),
        col("card.history.acct_open_date").alias("acct_open_date"),
        col("card.history.year_pin_last_changed").cast(IntegerType()).alias("year_pin_last_changed"),
        col("card.security.card_on_dark_web").cast(BooleanType()).alias("card_on_dark_web")
    )

elif src == "transactions":
    df_flattened = df_stream.select(
        col("transaction.id").cast(IntegerType()).alias("transaction_id"),
        col("transaction.date").cast(TimestampType()).alias("transaction_date"),
        regexp_replace(col("transaction.amount"), "[$,]", "").cast(DoubleType()).alias("amount"),
        col("transaction.client.client_id").cast(IntegerType()).alias("client_id"),
        col("transaction.client.card_id").cast(IntegerType()).alias("card_id"),
        col("transaction.client.use_chip").alias("use_chip"),
        col("transaction.merchant.merchant_id").cast(IntegerType()).alias("merchant_id"),
        col("transaction.merchant.city").alias("merchant_city"),
        col("transaction.merchant.state").alias("merchant_state"),
        col("transaction.merchant.zip").cast(DoubleType()).alias("zip"),
        col("transaction.merchant.mcc").cast(IntegerType()).alias("mcc"),
        col("transaction.errors").alias("errors")  
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing it to bronze layer

# COMMAND ----------

df_flattened.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .option("checkpointLocation", checkpoint)\
    .option("path", delta_path)\
    .start()