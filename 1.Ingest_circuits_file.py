# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Step 1- Read the CSV file using spark dataframe reader.

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#adding parameter to notebook

#dbutils.widgets.help() list all functions in this
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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

circuits_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')
circuits_df_with_header = spark.read.option("header",True).option("inferSchema",True).csv(f'{raw_folder_path}/{v_file_date}/circuits.csv', header='true')
circuits_df_with_our_schema = spark.read.option("header",True).schema(circuits_schema).csv(f'{raw_folder_path}/{v_file_date}/circuits.csv', header='true')
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
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())\
                       .withColumn("data_source",lit(v_data_source))\
                      .withColumn("file_date",lit(v_file_date))

#display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing data to datalake as parquet
# MAGIC

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits") to save file as parquet in adls
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits") # to save as a table
#with this we can read from both spark.read from adls and also as table



# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_processed.circuits

# COMMAND ----------

#%fs
#ls /mnt/formula1dl1811/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dl1811/processed/circuits") #to read parquet file
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")