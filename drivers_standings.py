# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"file_date='{v_file_date}'")\
                  .select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))



# COMMAND ----------

drivers_selected_df = race_results_df.select("race_year","race_name","driver_name","team","fastest_lap_time","points","driver_nationality")



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc,col,sum,when

race_position = Window.partitionBy("race_year","race_name").orderBy(desc("points"))

drivers_standings_df = drivers_selected_df.withColumn("rank",rank().over(race_position))

#display(drivers_standings_df.orderBy(col("race_year").desc()))

drivers_standings_with_wins = drivers_standings_df.withColumn("wins",when(col("rank")==1,1).otherwise(0))
drivers_final_standings_df =  drivers_standings_with_wins.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"),sum("wins").alias("wins"))
display(drivers_final_standings_df.orderBy(col("race_year").desc(),col("total_points").desc()))

# COMMAND ----------

drivers_rank = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
drivers_final_standings_with_rank_df = drivers_final_standings_df.withColumn("rank",rank().over(drivers_rank))
display(drivers_final_standings_with_rank_df.orderBy(col("race_year").desc(),col("total_points").desc()))

# COMMAND ----------

display(drivers_final_standings_with_rank_df.filter("race_year=2020"))

#drivers_final_standings_with_rank_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/drivers_standings")

#drivers_final_standings_with_rank_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.drivers_standings")

overwrite_partition(drivers_final_standings_with_rank_df,"f1_presentation","drivers_standings","race_year")