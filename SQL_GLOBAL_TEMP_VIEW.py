# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results.createOrReplaceGlobalTempView("race_results_view") # can be used in all notebooks unlike temp view

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.race_results_view where race_year = 2020