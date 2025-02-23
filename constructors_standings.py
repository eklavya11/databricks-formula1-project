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

#display(race_results_list)

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructors_selected_df = race_results_df.select("race_year","race_name","team","fastest_lap_time","points")

display(constructors_selected_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc,col,sum,when

constructor_position = Window.partitionBy("race_year","race_name").orderBy(desc("points"))

constructors_standings_df = constructors_selected_df.withColumn("rank",rank().over(constructor_position))

#display(drivers_standings_df.orderBy(col("race_year").desc()))

constructors_standings_with_wins = constructors_standings_df.withColumn("wins",when(col("rank")==1,1).otherwise(0))
constructors_final_standings_df =  constructors_standings_with_wins.groupBy("race_year","team").agg(sum("points").alias("total_points"),sum("wins").alias("wins"))
display(constructors_final_standings_df.orderBy(col("race_year").desc(),col("total_points").desc()))

# COMMAND ----------

constructors_rank = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
constructors_final_standings_with_rank_df = constructors_final_standings_df.withColumn("rank",rank().over(constructors_rank))
display(constructors_final_standings_with_rank_df.orderBy(col("race_year").desc(),col("total_points").desc()))

# COMMAND ----------

#display(constructors_final_standings_with_rank_df.filter("race_year=2020"))

#constructors_final_standings_with_rank_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructors_standings")
#constructors_final_standings_with_rank_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructors_standings")

overwrite_partition(constructors_final_standings_with_rank_df,"f1_presentation","constructors_standings","race_year")