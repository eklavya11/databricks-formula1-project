# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Pit_stops.json file

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

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("stop",StringType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("duration",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

#multiline True is important for reading multiline json file
pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline",True).json(f"/mnt/formula1dl1811/raw/{v_file_date}/pit_stops.json")


# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formula1dl1811/processed/pit_stops")
#pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

overwrite_partition(pit_stops_final_df,"f1_processed","pit_stops","race_id")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dl1811/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")