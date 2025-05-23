# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data lake using service principle.
# MAGIC 1. Get client_id, tenant_id, client_secret from key vault.
# MAGIC 2. Set spark config with App/client_id, Directory/tenant_id & secret.
# MAGIC 3. Call file system utility mount to mount the storage.
# MAGIC 4. Explore other file system utilities related to mount(list all mounts,unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-app-clientId')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenantId')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl1811.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl1811/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl1811/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl1811/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())