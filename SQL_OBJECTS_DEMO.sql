-- Databricks notebook source
CREATE DATABASE demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE demo --DESC also works

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo -- gives properties also

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES 

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

USE demo

-- COMMAND ----------

--Managed table is stored in hive metastore with data, when we drop managed table, data is also dropped
--When we drop External table then data is not drop only data related to table like fields, columns, schema, meta data is dropped as data is stored in adls gen 2

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_table") #managed table

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE demo

-- COMMAND ----------

DESC EXTENDED race_results_table

-- COMMAND ----------

SELECT * FROM race_results_table WHERE race_year = 2020

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS race_results_sql
AS
SELECT * FROM race_results_table WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM race_results_sql

-- COMMAND ----------

DROP TABLE race_results_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df_ext = spark.read.parquet(f"{presentation_folder_path}/race_results")#external table
-- MAGIC race_results_df_ext.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_table_ext").saveAsTable("race_results_table_ext_py")

-- COMMAND ----------

SELECT * FROM race_results_table_ext_py

-- COMMAND ----------

DESC EXTENDED race_results_table_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_sql = spark.sql("SELECT * FROM race_results_table_ext_py WHERE race_year =2020")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_sql.write.mode("overwrite").format("parquet").option("path",f"{presentation_folder_path}/race_results_table_ext_sql").saveAsTable("race_results_table_ext_sql")

-- COMMAND ----------

DESC EXTENDED race_results_table_ext_sql

-- COMMAND ----------

SELECT * FROM race_results_table_ext_sql

-- COMMAND ----------

--Creating external table using SQL
CREATE TABLE IF NOT EXISTS race_details_sql
(
  race_year INT,
  race_name STRING,
  driver_name STRING
)
USING PARQUET
LOCATION "/mnt/formula1dl1811/presentation/race_details_sql"

-- COMMAND ----------

DESC race_details_sql

-- COMMAND ----------

SELECT COUNT(1) FROM race_details_sql

-- COMMAND ----------

INSERT INTO race_details_sql
SELECT race_year, race_name, driver_name FROM race_results_table_ext_py where race_year =2020

-- COMMAND ----------

SELECT * FROM demo.race_results_table_ext_py

-- COMMAND ----------

--Temp view is available only in spark session and cant be accessed by other notebooks
--while a global temp view is available in spark application in a process or a batch and can be used by all notebooks for that process
--permanent view is available always, and is present in hive metastore even after we detach with cluster.
--SHOW TABLES IN global_temp;
--DESC EXTENDED demo.race_details_sql;
--DESC EXTENDED demo.race_results_table;
--DESC EXTENDED demo.race_results_table_ext_py;
--DESC EXTENDED demo.race_results_table_ext_sql;
--SELECT DISTINCT race_year FROM race_details_sql

-- COMMAND ----------

--Temp view
CREATE OR REPLACE TEMP VIEW tmp_race_results_view
AS
SELECT * FROM demo.race_results_table WHERE race_year = 2018

-- COMMAND ----------

--Global Temp View
CREATE OR REPLACE GLOBAL TEMP VIEW gtemp_race_results_view
AS
SELECT * FROM demo.race_results_table WHERE race_year = 2012

-- COMMAND ----------

--SHOW TABLES IN global_temp

-- COMMAND ----------

--permanent view
CREATE OR REPLACE VIEW demo.P_race_results_view
AS
SELECT * FROM demo.race_results_table WHERE race_year = 2002

-- COMMAND ----------

SHOW TABLES IN demo