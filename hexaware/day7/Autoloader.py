# Databricks notebook source
inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"
outputpath="dbfs:/mnt/asadlsad/processeddata/outputautoloader"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/reshma/schemalocation")
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/reshma/checkpoint")
.option("path",f"{outputpath}/reshma/files")
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.firstauto

# COMMAND ----------

# MAGIC %md
# MAGIC Infering column types

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table stream.firstauto

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/reshma/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/reshma/checkpoint")
.option("path",f"{outputpath}/reshma/files")
.table("stream.firstauto")
)

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/reshma/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/reshma/checkpoint")
.option("path",f"{outputpath}/reshma/files")
.option("mergeSchema",True)
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.firstauto

# COMMAND ----------


