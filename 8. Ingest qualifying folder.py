# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest multiple multi line json files from qualifying folder

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

qualifying_schema = StructType(fields=[StructField("qualifyingId",IntegerType(),False),
                                      StructField("raceId",IntegerType(),True),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("constructorId",IntegerType(),True),
                                      StructField("number",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("q1",StringType(),True),
                                      StructField("q2",StringType(),True),
                                      StructField("q3",StringType(),True)])

# COMMAND ----------

#reading files from folder
qualifying_df = spark.read.option("multiline",True).schema(qualifying_schema).json(f"/mnt/formula1dl1811/raw/{v_file_date}/qualifying")


# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyingId","qualifying_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1dl1811/processed/qualifying")

#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

overwrite_partition(qualifying_final_df,"f1_processed","qualifying","race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")