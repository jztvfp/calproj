# Databricks notebook source
# MAGIC %md
# MAGIC # Import Data
# MAGIC
# MAGIC Import the source data from the volume

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

source = spark.conf.get("source")


# COMMAND ----------

@dlt.view(
  name = "raw_data",
  comment="Raw data from the turbines"
)
def data_raw():
  df = (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", f"{source}/landing/inferred_schema")
      .option("rescuedDataColumn", "_rescued_data")
      .option("header","true")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaHints", "timestamp timestamp")
      .load(source +"/")
  )
  return df.withColumn("filename", F.col("_metadata.file_path"))
  


@dlt.table(
    name = "bronze_data",
    comment="New readings - exclude rows with dates in the future",
    table_properties = {
      "quality": "bronze"
      }
)
def data_bronze():
  df = dlt.read_stream("raw_data")
  return df.withColumn("processedtime",F.current_timestamp())
  
                            
