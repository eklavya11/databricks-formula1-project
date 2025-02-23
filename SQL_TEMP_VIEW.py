# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("race_results_sql")
#temp view only exists in  a session and cant be used by other notebooks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_results_sql where race_year = 2020 order by points desc

# COMMAND ----------

var_year = 2019

# COMMAND ----------

temp_view_df = spark.sql(f"SELECT * FROM race_results_sql where race_year = {var_year} order by points desc")

# COMMAND ----------

display(temp_view_df)