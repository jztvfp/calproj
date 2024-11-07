-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Check for schema issues
-- MAGIC

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE bronze_turbine_data (
  CONSTRAINT incorrect_data_removed EXPECT (not_empty_rescued_data = 0) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: bronze table properly drops row with incorrect schema"
AS SELECT count(*) as not_empty_rescued_data from project.bronze.turbine_data  where _rescued_data is not null 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Check the Bronze table

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_turbine_data_bronze (
  CONSTRAINT null_timestamps_removed EXPECT (null_ts = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT null_ids_removed EXPECT (null_id_count = 0) ON VIOLATION FAIL UPDATE  
)
COMMENT "TEST: check bronze table removes null ids and timestamps"
AS (
  WITH
   timestamp_test AS (SELECT count(*) AS null_ts  FROM project.bronze.turbine_data  WHERE timestamp IS NULL),
   id_test AS (SELECT count(*) AS null_id_count  FROM project.bronze.turbine_data  WHERE turbine_id IS NULL)
  SELECT * from timestamp_test, id_test)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Check the Silver table

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_turbine_data_silver (
  CONSTRAINT winddir_OK EXPECT (imposs_dir = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT windspeed_OK EXPECT (neg_speed = 0) ON VIOLATION FAIL UPDATE  
)
COMMENT "TEST: check silver table has OK Winddirection and speed"
AS (
  WITH
   winddir_test AS (SELECT count(*) AS imposs_dir FROM project.silver.turbine_data WHERE winddirection NOT BETWEEN 0 and 360),
   windspeed_test AS (SELECT count(*) AS neg_speed  FROM  project.silver.turbine_data  WHERE windspeed < 0)
  SELECT * from winddir_test, windspeed_test)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Testing Gold Primary key uniqueness
-- MAGIC

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_mv_turbine_data_gold (
  CONSTRAINT pk_must_be_unique EXPECT (duplicate = 1) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: check that gold table only contains unique customer id"
AS SELECT count(*) as duplicate, TurbineNumber,ReadingDate FROM project.gold.mv_turbine_data GROUP BY all
