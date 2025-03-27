# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client id, Directory/ Tenant id & Secret
# MAGIC 3. Call file system utility to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)
# MAGIC
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-tenantid")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlmm24.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlmm24/demo",
  extra_configs = configs)

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlmm24/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlmm24/demo/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Mounting Commands

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dlmm24/demo")
