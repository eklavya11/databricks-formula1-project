# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS token using secret key
# MAGIC 1. Set the spark configuration for SAS token
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

#dbutils.secrets.listScopes()
#dbutils.secrets.help()
#dbutils.secrets.list("formula1-scope")
#dbutils.secrets.

formula1_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope',key='formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1811.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl1811.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl1811.dfs.core.windows.net",formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfs://demo@formula1dl1811.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

