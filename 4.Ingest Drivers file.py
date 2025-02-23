# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON file using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])
drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),StructField("code",StringType(),True),
                                    StructField("name",name_schema),StructField("dob",DateType(),True),StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"/mnt/formula1dl1811/raw/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
drivers_renamed_df = drivers_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dl1811/processed/drivers")
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dl1811/processed/drivers.parquet"))

# COMMAND ----------

dbutils.notebook.exit("Success")