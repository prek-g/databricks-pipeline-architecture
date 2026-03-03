# Databricks notebook source
src_array = [
    {"src" : "cards"},
    {"src" : "users"},
    {"src" : "transactions"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value = src_array)