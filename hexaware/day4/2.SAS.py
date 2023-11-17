# Databricks notebook source
?sv=2022-11-02&ss=bfqt&srt=c&sp=rwdlacupyx&se=2023-11-14T11:08:50Z&st=2023-11-14T03:08:50Z&spr=https&sig=mTZv4tFDmNTeRFgaRALYW43pdXdantdYw5wN9mXxw3s%3D

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sasdlsrs.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sasdlsrs.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.tokensasdlsrs.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=c&sp=rwdlacupyx&se=2023-11-14T11:08:50Z&st=2023-11-14T03:08:50Z&spr=https&sig=mTZv4tFDmNTeRFgaRALYW43pdXdantdYw5wN9mXxw3s%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@sasdlsrs.dfs.core.windows.net/fact/")

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsad/processeddata")

# COMMAND ----------

# MAGIC %fs ls /mnt/asadlsad/

# COMMAND ----------


