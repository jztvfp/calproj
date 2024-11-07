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

CREATE TEMPORARY LIVE TABLE TEST_turbine_data_bronze (
  CONSTRAINT keep_all_rows              EXPECT (num_rows = 4)      ON VIOLATION FAIL UPDATE, 
  CONSTRAINT email_should_be_anonymized EXPECT (clear_email = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT null_ids_removed           EXPECT (null_id_count = 0) ON VIOLATION FAIL UPDATE  
)
COMMENT "TEST: check silver table removes null ids and anonymize emails"
AS (
  WITH
   rows_test  AS (SELECT count(*) AS num_rows       FROM live.user_silver_dlt),
   email_test AS (SELECT count(*) AS clear_email    FROM live.user_silver_dlt  WHERE email LIKE '%@%'),
   id_test    AS (SELECT count(*) AS null_id_count  FROM live.user_silver_dlt  WHERE id IS NULL)
  SELECT * from email_test, id_test, rows_test)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Check the Silver table

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_user_silver_dlt_anonymize (
  CONSTRAINT keep_all_rows              EXPECT (num_rows = 4)      ON VIOLATION FAIL UPDATE, 
  CONSTRAINT email_should_be_anonymized EXPECT (clear_email = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT null_ids_removed           EXPECT (null_id_count = 0) ON VIOLATION FAIL UPDATE  
)
COMMENT "TEST: check silver table removes null ids and anonymize emails"
AS (
  WITH
   rows_test  AS (SELECT count(*) AS num_rows       FROM live.user_silver_dlt),
   email_test AS (SELECT count(*) AS clear_email    FROM live.user_silver_dlt  WHERE email LIKE '%@%'),
   id_test    AS (SELECT count(*) AS null_id_count  FROM live.user_silver_dlt  WHERE id IS NULL)
  SELECT * from email_test, id_test, rows_test)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Testing Primary key uniqueness
-- MAGIC
-- MAGIC Finally, we'll enforce uniqueness on the gold table to avoid any duplicates

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_user_gold_dlt (
  CONSTRAINT pk_must_be_unique EXPECT (duplicate = 1) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: check that gold table only contains unique customer id"
AS SELECT count(*) as duplicate, id FROM live.user_gold_dlt GROUP BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it. All we have to do now is run the full pipeline.
-- MAGIC
-- MAGIC If one of the condition defined in the TEST table fail, the test pipeline expectation will fail and we'll know something need to be fixed!
-- MAGIC
-- MAGIC You can open the <a dbdemos-pipeline-id="dlt-test" href="#joblist/pipelines/6aced2ad-9e46-4b62-b8c8-129b99363d4d">Delta Live Table Pipeline for unit-test</a> to see the tests in action
