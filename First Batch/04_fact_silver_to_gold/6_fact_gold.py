# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("source_schema", "")

dbutils.widgets.text("source_obj", "")

dbutils.widgets.text("business_key", "")

dbutils.widgets.text("sk_col", "")


# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")

source_obj = dbutils.widgets.get("source_obj")

business_key = dbutils.widgets.get("business_key")

sk_col = dbutils.widgets.get("sk_col")

# COMMAND ----------

df_fact = spark.table("workspace.silver.transactions_data")

# COMMAND ----------

silver_df = (
    df_fact.withColumn("customer_sk", lit(None).cast(StringType()))
    .withColumn("card_sk", lit(None).cast(StringType()))
    .withColumn("merchant_sk", lit(None).cast(StringType()))
)

# COMMAND ----------

if not spark.catalog.tableExists("workspace.gold.fact_transactions"):
    print("Creating new transaction fact table in the gold schema")

    silver_df.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("workspace.gold.fact_transactions")

    print("Fact table created")

else:
    delta_fact = DeltaTable.forName(spark, "workspace.gold.fact_transactions")
    
    (
        delta_fact.alias("t").merge(
        silver_df.alias("s"),
        "t.transaction_id = s.transaction_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("New rows were inserted successfully")

# COMMAND ----------

source_table = f"{source_schema}.{source_obj}"

# COMMAND ----------

dim_table = ( spark.table(source_table)
    .filter(col("is_active") == True)
    .select(
        col(business_key), 
        col("surrogate_key").alias(sk_col)
        )
)

delta_target = DeltaTable.forName(spark,"workspace.gold.fact_transactions")

update_condition = f"t.{business_key} = s.{business_key} AND t.{sk_col} IS NULL"
set_update = {sk_col : f"s.{sk_col}"}
(
    delta_target.alias("t").merge(
        dim_table.alias("s"),
        update_condition
    )
    .whenMatchedUpdate(
    set = set_update
    )
    .execute()
)