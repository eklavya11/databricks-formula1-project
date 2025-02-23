# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results.json file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                          StructField("raceId",IntegerType(),False),
                                          StructField("driverId",IntegerType(),False),
                                          StructField("constructorId",IntegerType(),False),
                                          StructField("number",IntegerType(),True),
                                          StructField("grid",IntegerType(),False),
                                          StructField("position",IntegerType(),True),
                                          StructField("positionText",StringType(),False),
                                          StructField("positionOrder",IntegerType(),False),
                                          StructField("points",FloatType(),False),
                                          StructField("laps",IntegerType(),False),
                                          StructField("time",StringType(),True),
                                          StructField("milliseconds",IntegerType(),True),
                                          StructField("fastestLap",IntegerType(),True),
                                          StructField("rank",IntegerType(),True),
                                          StructField("fastestLapTime",StringType(),True),
                                          StructField("fastestLapSpeed",StringType(),True),
                                          StructField("StatusId",IntegerType(),False)])

# COMMAND ----------

result_df = spark.read.schema(results_schema).json(f"/mnt/formula1dl1811/raw/{v_file_date}/results.json")

# COMMAND ----------

result_final_df = result_df.withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("positionText","position_text")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("fastestLap","fastest_lap")\
    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))\
    .drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC METHOD 1 FOR INCREMENTAL LOAD

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

    

# COMMAND ----------


#result_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1dl1811/processed/#results")

#result_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")



# COMMAND ----------

# MAGIC %md
# MAGIC METHOD 2 FOR INCREMENTAL LOAD -- more efficient

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic") #only overwrites the partiton

# COMMAND ----------

# results_final_df = result_final_df.select("result_id","driver_id","constructor_id","number","grid","position","position_text","position_order","points","laps","time","milliseconds","fastest_lap","rank","fastest_lap_time","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

overwrite_partition(result_final_df,"f1_processed","results","race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1), race_id FROM f1_processed.results GROUP BY race_id ORDER BY race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")