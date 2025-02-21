# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount ADLS Container for the Project
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client id, Directory/ Tenant id & Secret
# MAGIC 3. Call file system utility to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get secrets from KeyVault
    client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-id")
    tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-tenantid")
    client_secret = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-secret")  
    #Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret":client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    #Mount Storage Account Container
    ## Unmount to mount point if it already exists

    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    #Display the lists of mounts
    display(dbutils.fs.mounts())



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Mount Raw Container

# COMMAND ----------

mount_adls('formula1dlmm24', 'raw')

# COMMAND ----------

mount_adls('formula1dlmm24', 'processed')

# COMMAND ----------

mount_adls('formula1dlmm24', 'presentation')

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
