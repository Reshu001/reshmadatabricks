# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/sasdlsrs/bronze/delta

# COMMAND ----------

df_drivers=spark.read.json("dbfs:/mnt/asadlsad/processeddata/raw/drivers.json")
df_pitstop=spark.read.option("multiline",True).json("dbfs:/mnt/asadlsad/processeddata/raw/pit_stops.json")

# COMMAND ----------

df_drivers.join(df_pitstop).display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId").display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/asadlsad/processeddata/raw/Nulls.csv",header=True,inferSchema=True)

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

df1.dropna("all",subset="id").display()

# COMMAND ----------

df1.fillna({"id":0,"name":"unknown","Marks":49,"placed":False}).display()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://processeddata@sasdlsrs.blob.core.windows.net",
  mount_point = "/mnt/sasdlsrs/processeddata",
  extra_configs = {"fs.azure.account.key.sasdlsrs.blob.core.windows.net":"QdchngrL7bgB3I2/wEX0dz3mXysA2JLFZ4e5BvRvEQxXEmLKqt0DsGLZvn123xDeMZV32ld83AJL+AStzrFlXQ=="})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://bronze@sasdlsrs.blob.core.windows.net",
  mount_point = "/mnt/sasdlsrs/bronze",
  extra_configs = {"fs.azure.account.key.sasdlsrs.blob.core.windows.net":"QdchngrL7bgB3I2/wEX0dz3mXysA2JLFZ4e5BvRvEQxXEmLKqt0DsGLZvn123xDeMZV32ld83AJL+AStzrFlXQ=="})

# COMMAND ----------


