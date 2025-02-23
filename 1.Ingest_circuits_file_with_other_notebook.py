# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Step 1- Read the CSV file using spark dataframe reader.

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True),])

# COMMAND ----------

circuits_df = spark.read.csv(f'{raw_folder_path}/circuits.csv')
circuits_df_with_header = spark.read.option("header",True).option("inferSchema",True).csv(f'{raw_folder_path}/circuits.csv', header='true')
circuits_df_with_our_schema = spark.read.option("header",True).schema(circuits_schema).csv(f'{raw_folder_path}/circuits.csv', header='true')
# infer schema option is not good as it slows down the process, not recommended for production and can be used in dev environment instead.

# COMMAND ----------

#%fs 
#ls /mnt/formula1dl1811/raw - listing data using fs magic command

# COMMAND ----------

#type(circuits_df) #shows type

# COMMAND ----------

#circuits_df.show() #displays the first 20 rows, but this truncates values

# COMMAND ----------

#display(circuits_df)#this does not truncate data

# COMMAND ----------

#display(circuits_df_with_header)

# COMMAND ----------

#circuits_df_with_header.printSchema() # shows schema
#circuits_df_with_our_schema.printSchema()

# COMMAND ----------

#circuits_df_with_header.describe().show() #descibe gives details of data

# COMMAND ----------

    #display(circuits_df_with_our_schema)



# COMMAND ----------

# MAGIC %md
# MAGIC ##Select only required columns

# COMMAND ----------

#four ways of selecting columns
circuits_selected_df = circuits_df_with_our_schema.select("circuitId","circuitRef","name")

circuits_selected_df = circuits_df_with_our_schema.select(circuits_df_with_our_schema.circuitId,circuits_df_with_our_schema.circuitRef,circuits_df_with_our_schema.name)

circuits_selected_df = circuits_df_with_our_schema.select(circuits_df_with_our_schema["circuitId"],circuits_df_with_our_schema["circuitRef"])

circuits_selected_df = circuits_df_with_our_schema.select(circuits_df_with_our_schema.columns[0],circuits_df_with_our_schema.columns[1])

from pyspark.sql.functions import col

circuits_selected_df = circuits_df_with_our_schema.select(col("circuitId"),col("circuitRef"),col("country").alias("race_country"))#alias is used to rename the column
#display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df_with_our_schema.select(col("circuitId"),col("circuitRef"),col("country"),col("name"),col("location"),col("lat"),col("lng"),col("alt"))



# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename the column as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")

#display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##add ingestion date to dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp()).withColumn("env",lit("production")) #makes a column env and populated it with production using lit for all rows
circuits_final_df = add_ingestion_date(circuits_renamed_df)

#display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing data to datalake as parquet
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")



# COMMAND ----------

#%fs
#ls /mnt/formula1dl1811/processed/circuits

# COMMAND ----------

#df = spark.read.parquet("/mnt/formula1dl1811/processed/circuits") #to read parquet file
#display(df)