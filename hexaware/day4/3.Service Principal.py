# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('hexasecrets')

# COMMAND ----------

dbutils.secrets.get(scope="hexasecrets",key="clientsecretid")

# COMMAND ----------

dbutils.secrets.get(scope="hexasecrets",key="tenantsecretid")
dbutils.secrets.get(scope="hexasecrets",key="secretkey")

# COMMAND ----------

container_name       = "bronze"
storage_account_name = "sasdlsrs"
client_id            = dbutils.secrets.get(scope="hexasecrets",key="clientsecretid")
tenant_id            = dbutils.secrets.get(scope="hexasecrets",key="tenantsecretid")
client_secret        = dbutils.secrets.get(scope="hexasecrets",key="secretkey")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------


