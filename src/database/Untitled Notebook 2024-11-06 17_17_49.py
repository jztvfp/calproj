# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE MATERIALISED VIEW
# MAGIC Create a materialised view using dynamic SQL

# COMMAND ----------

table_name = "project.silver.turbine_data"

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC USE CATALOG project;
# MAGIC USE DATABASE gold;
# MAGIC

# COMMAND ----------

query = """
CREATE OR REPLACE MATERIALIZED VIEW project.gold.mv_turbine_data
    PARTITIONED BY (TurbineNumber, ReadingDate)
    AS 
WITH turbine_data AS
( 
  SELECT
    TurbineNumber,
    ReadingDate,
    WindSpeed,
    WindDirection,
    PowerOutput,
    ROUND(AVG(PowerOutput) OVER (PARTITION BY TurbineNumber), 4) AS LongTermAveragePowerOutput,
    ROUND(STDDEV(PowerOutput) OVER (PARTITION BY TurbineNumber), 4) AS LongTermStandardDeviationPowerOutput,
    MIN(PowerOutput) OVER (
      PARTITION BY TurbineNumber 
      ORDER BY ReadingDate 
      RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW
    ) AS MinimumPowerOutputPreviousDay,
    MAX(PowerOutput) OVER (
      PARTITION BY TurbineNumber 
      ORDER BY ReadingDate 
      RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW
    ) AS MaximumPowerOutputPreviousDay,
    ROUND(AVG(PowerOutput) OVER (
      PARTITION BY TurbineNumber 
      ORDER BY ReadingDate 
      RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW
    ), 2) AS AveragePowerOutputPreviousDay
  FROM 
    project.silver.silver_data_table
)
SELECT 
  *,
  ROUND(ABS(AveragePowerOutputPreviousDay - LongTermAveragePowerOutput), 4) AS PowerOutputDeviationFromLongTermAverage,
  CASE 
    WHEN AveragePowerOutputPreviousDay - LongTermAveragePowerOutput > 2 * LongTermStandardDeviationPowerOutput 
    THEN 1 
    ELSE 0 
  END AS IsAnomaly 
FROM 
  turbine_data
"""

spark.sql(query, id1_val="id016").show()
