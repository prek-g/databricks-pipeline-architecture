# Databricks notebook source
# MAGIC %md
# MAGIC ### Dependencies

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic variables

# COMMAND ----------

dbutils.widgets.text("source_schema", "")

dbutils.widgets.text("source_obj", "")

dbutils.widgets.text("target_schema", "")

dbutils.widgets.text("target_obj", "")

dbutils.widgets.text("cdc_col", "")

dbutils.widgets.text("key_col_list", "")

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")

source_obj = dbutils.widgets.get("source_obj")

target_schema = dbutils.widgets.get("target_schema")

target_obj = dbutils.widgets.get("target_obj")

cdc_col = dbutils.widgets.get("cdc_col")

key_col_list = eval(dbutils.widgets.get("key_col_list"))

# COMMAND ----------

source_table = f"{source_schema}.{source_obj}"
target_table = f"{target_schema}.{target_obj}"

# COMMAND ----------

# MAGIC %md
# MAGIC Declaring the source table.

# COMMAND ----------

df_source = spark.table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The SCD2 logic

# COMMAND ----------

if not spark.catalog.tableExists(target_table):

    print(f"Creating the initial table {target_table} from {source_table}")

    df_init = (df_source
            .withColumn("surrogate_key", expr("uuid()"))
            .withColumn("is_active", lit(True))
            .withColumn("start_date", current_timestamp())
            .withColumn("end_date", to_timestamp(lit("3000-01-01")))
                )
    df_init.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(target_table)

else:
        
    print(f"Updating the existing table {target_table}")

    # Preparing the delta object which will be the target table to perform the merge
    target_delta = DeltaTable.forName(spark, target_table)
    
    # Preparing the source dataframe with SCD columns
    df_source_prepared = (df_source
            .withColumn("surrogate_key", expr("uuid()"))
            .withColumn("is_active", lit(True))
            .withColumn("start_date", current_timestamp())
            .withColumn("end_date", to_timestamp(lit("3000-01-01")))
                )
    
    # Building the merge condition for key columns only
    join_condition = " AND ".join([f"t.{c} = s.{c}" for c in key_col_list])

    # Identifying the columns that should trigger an update
    exclude_cols = set(key_col_list + 
                       [cdc_col, "surrogate_key", "is_active", "start_date", "end_date"])
    
    # Extracting df_source_prepared columns that arent listed in exclude_cols 
    compare_cols = [c for c in df_source_prepared.columns if c not in exclude_cols]

    # If any of those columns have changed (a dimension change) then apply the change condition
    change_condition = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in compare_cols])

    # Setting the condition to expire old rows
    update_condition = f"s.{cdc_col} > t.{cdc_col} AND ({change_condition})"

    # Expiring matched rows
    
    target_delta.alias("t").merge(
        df_source_prepared.alias("s"),
        f"{join_condition} AND t.is_active = true"
    )\
        .whenMatchedUpdate(
            condition = update_condition,
            set = {
                "is_active" : "false",
                "end_date" : "current_timestamp()"
            }
        ).execute()
    print("Inserting updated & new records")
    df_insert = df_source_prepared.alias("s").join(
    target_delta.toDF().filter("is_active = true").alias("t"),
        expr(join_condition),
        how = "left_outer"
    ).filter(
        (col("t.surrogate_key").isNull())
    ).select("s.*")

insert_count = df_insert.count()
print(f"Total records to be appended: {insert_count}")

# COMMAND ----------

    df_insert.write.format("delta")\
        .mode("append")\
        .option("mergeSchema", "true")\
        .saveAsTable(target_table)

    print("Merge completed")
print("SCD2 completed") 