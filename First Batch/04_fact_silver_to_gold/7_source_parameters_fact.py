# Databricks notebook source
fact_array = [
    {
        "source_schema" : "gold",
        "source_obj" : "dim_customers",
        "business_key" : "customer_id",
        "sk_col" : "customer_sk"
    },
    {
        "source_schema" : "gold",
        "source_obj" : "dim_cards",
        "business_key" : "card_id",
        "sk_col" : "card_sk"
    },
    {
        "source_schema" : "gold",
        "source_obj" : "dim_merchants",
        "business_key" : "merchant_id",
        "sk_col" : "merchant_sk"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value = fact_array)