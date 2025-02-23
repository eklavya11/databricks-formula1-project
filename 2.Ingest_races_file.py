# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Races.CSV file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True), 
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",StringType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True),])

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, concat

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv",header='true')

#adding race timestamp column
races_transformed_df = races_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))\
                       .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------


races_selected_df = races_transformed_df.select("raceId","year","round","circuitId","name","race_timestamp","ingestion_date")

races_final_df = races_selected_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("year","race_year")\
    .withColumnRenamed("circuitId","circuit_id")\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))
                            

#display(races_final_df)

# COMMAND ----------

#races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")
races_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

#df = spark.read.parquet("/mnt/formula1dl1811/processed/races")
#df.show()

# COMMAND ----------

dbutils.notebook.exit("Success")