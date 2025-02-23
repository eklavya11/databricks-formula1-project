# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
races_df = spark.read.parquet(f"{processed_folder_path}/races")
results_df = spark.read.parquet(f"{processed_folder_path}/results").filter(f"file_date='{v_file_date}'")


# COMMAND ----------

race_results_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id,"inner")\
                  .join(results_df,races_df.race_id==results_df.race_id,"inner")\
                  .join(drivers_df,drivers_df.driver_id==results_df.driver_id,"inner")\
                  .join(constructors_df,constructors_df.constructor_id==results_df.constructor_id,"inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date ,col
races_results_selected_df = race_results_df.select(races_df.race_year,races_df.name.alias("race_name"),
                                                   to_date(races_df.race_timestamp).alias("race_date"),
                                                   circuits_df.location.alias("circuit_location"),
                                                   drivers_df.name.alias("driver_name"),drivers_df.number.alias("driver_number"),
                                                   drivers_df.nationality.alias("driver_nationality"),
                                                   constructors_df.name.alias("team"),
                                                   results_df.grid,results_df.fastest_lap_time,results_df.time.alias("race_time"),results_df.points,results_df.file_date,results_df.race_id)\
                                                   .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(races_results_selected_df)

# COMMAND ----------

race_results_final_df = races_results_selected_df.filter(col("race_date")=="2020-12-13")
display(race_results_final_df.select("driver_name","driver_number","team","grid","fastest_lap_time","race_time","points").orderBy(col("points").desc()))

#races_results_selected_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
#races_results_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

overwrite_partition(races_results_selected_df,"f1_presentation","race_results","race_id")


