# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark configuration fs.azure.account.key
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dl1811.dfs.core.windows.net",
                 "i9eBisHHu/kKNBSlYmiBSWNgyheYZsBjm6z0zAyw5Jw888NFY2QTj//zLUVpq9M+oK4y6QfTe1QA+AStYlt+vg==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1811.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

