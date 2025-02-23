# Databricks notebook source
spark.conf.set("fs.azure.account.key.formula1dl1811.dfs.core.windows.net","i9eBisHHu/kKNBSlYmiBSWNgyheYZsBjm6z0zAyw5Jw888NFY2QTj//zLUVpq9M+oK4y6QfTe1QA+AStYlt+vg==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))