# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark configuration for SAS token
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1811.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl1811.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl1811.dfs.core.windows.net","sp=rl&st=2025-01-13T07:16:55Z&se=2025-01-13T15:16:55Z&spr=https&sv=2022-11-02&sr=c&sig=Jiti1P9P2jCG027X2KjckfCEf52x1pbz%2BLpNLamhoxM%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1811.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

