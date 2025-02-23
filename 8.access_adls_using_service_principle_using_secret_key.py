# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principle- using secret key
# MAGIC 1. Register Azure AD application/Service Principle
# MAGIC 2. Generate a secret/password for application.
# MAGIC 3. Set spark config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the data lake.

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-app-clientId')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenantId')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

# COMMAND ----------

#service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>") - not needed

spark.conf.set("fs.azure.account.auth.type.formula1dl1811.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl1811.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl1811.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl1811.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl1811.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1811.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1811.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

