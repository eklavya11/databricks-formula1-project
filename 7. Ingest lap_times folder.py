# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest multiple csv files lap_times folder

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

#reading files from folder
lap_times_df = spark.read.schema(lap_times_schema).csv(f"/mnt/formula1dl1811/raw/{v_file_date}/lap_times")


# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1dl1811/processed/lap_times")
#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

overwrite_partition(lap_times_final_df,"f1_processed","lap_times","race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")