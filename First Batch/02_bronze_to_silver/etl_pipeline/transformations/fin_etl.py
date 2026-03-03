import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

################### The fact table

@dlt.table(
    name = "stage_transactions"
)

def stage_transactions():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/finance_project/transactions_data/query/")
    return df

@dlt.view(
    name = "trans_transactions"
)

def trans_transactions():
    df = spark.readStream.table("stage_transactions")
    df = df.withColumn("modifiedDate", current_timestamp())\
        .withColumnRenamed("client_id", "customer_id")\
        .drop("merchant_city", "merchant_state", "zip", "mcc" , "errors")
    return df

rules = {
    "rule_1" : "transaction_id IS NOT NULL",
    "rule_2" : "customer_id IS NOT NULL",
    "rule_3" : "card_id IS NOT NULL",
    "rule_4" : "merchant_id IS NOT NULL"
}

@dlt.table (
    name = "transactions_data"
)
@dlt.expect_all_or_drop(rules)
def transactions_data():
    df = spark.readStream.table("trans_transactions")
    return df

##################### dim tables - merchants_data

@dlt.view (
    name = "trans_merchants"
    )
@dlt.expect_all_or_drop({"merchant_id": "merchant_id IS NOT NULL"})

def trans_merchants():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/finance_project/transactions_data/query/")

    df = df.select("merchant_id", "merchant_city", "merchant_state", "zip", "mcc")\
            .withColumn("modifiedDate", current_timestamp())
    return df

dlt.create_streaming_table("merchants_data")
dlt.create_auto_cdc_flow (
    target = "merchants_data",
    source = "trans_merchants",
    keys = ["merchant_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

################################# cards_data

@dlt.view (
    name = "trans_cards"
)

@dlt.expect_all_or_drop({"card_id": "card_id IS NOT NULL"})

def trans_cards():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/finance_project/cards_data/query/")
    df = df.withColumn("expires", to_date(concat_ws("-", split(col("expires"), "/").getItem(1), split(col("expires"), "/").getItem(0), lit("01"))))\
            .withColumn("acct_open_date", to_date(concat_ws("-", split(col("acct_open_date"), "/").getItem(1), split(col("acct_open_date"), "/").getItem(0), lit("01"))))\
            .withColumn("modifiedDate", current_timestamp())
            
    return df
dlt.create_streaming_table("cards_data")
dlt.create_auto_cdc_flow (
    target = "cards_data",
    source = "trans_cards",
    keys = ["card_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

################################# users_data

@dlt.view (
    name = "trans_customers"
)
@dlt.expect_all_or_drop({"customer_id" : "customer_id IS NOT NULL"})

def trans_customers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/finance_project/users_data/query/")
    df = df.withColumn("modifiedDate", current_timestamp())
    return df
dlt.create_streaming_table("customers_data")
dlt.create_auto_cdc_flow(
    target = "customers_data",
    source = "trans_customers",
    keys = ["customer_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)
