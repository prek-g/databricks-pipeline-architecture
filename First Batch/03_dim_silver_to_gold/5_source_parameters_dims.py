# Databricks notebook source
dim_array= [
    {
        "source_schema" : "silver",
        "source_obj" : "merchants_data",
        "target_schema" : "gold",
        "target_obj" : "dim_merchants",
        "cdc_col" : "modifiedDate",
        "key_col_list" : ["merchant_id"]
        },
    
    {
        "source_schema" : "silver",
        "source_obj" : "cards_data",
        "target_schema" : "gold",
        "target_obj" : "dim_cards",
        "cdc_col" : "modifiedDate",
        "key_col_list" : ["card_id"]  
    }, 

    {
        "source_schema" : "silver",
        "source_obj" : "customers_data",
        "target_schema" : "gold",
        "target_obj" : "dim_customers",
        "cdc_col" : "modifiedDate",
        "key_col_list" : ["customer_id"]       
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value=dim_array)