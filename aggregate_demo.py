# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

race_results_df = race_results_df.filter("race_year = 2020")
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,countDistinct,col

display(race_results_df.select(count("*").alias("total_records"),countDistinct("driver_name").alias("total_drivers")))

# COMMAND ----------

display(race_results_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points").alias("Total_points"),countDistinct("race_name").alias("Total_races")))

# COMMAND ----------

display(race_results_df.select(sum("points").alias("Total_points"),col("driver_name")).groupBy("driver_name"))