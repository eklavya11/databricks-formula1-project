# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using clustered scope authentication
# MAGIC 1. Set the spark configuration fs.azure.account.key in cluster
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1811.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

