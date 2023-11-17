# Databricks notebook source
Batch Read:
    df=spark.read.csv("path")
write:
    df=spark.write.save()
    df=spark.write.saveAsTable("tblename")

Streaming
df=spark.readStream.csv("path")
df.writeStream.saveAsTable("tblname")

databricks(Autoloader)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/inputstream/csv/

# COMMAND ----------

inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_sch=StructType([StructField("Id",IntegerType()),
                      StructField("Name",StringType()),
                      StructField("Gender",StringType()),
                      StructField("Salary",IntegerType()),
                      StructField("Country",StringType()),
                      StructField("Date",StringType()),
])

# COMMAND ----------

df=spark.readStream.option("header",True).schema(users_sch).csv(f"{inputpath}")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp())

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema stream;

# COMMAND ----------

outputpath="dbfs:/mnt/asadlsad/processeddata/outputstream"

# COMMAND ----------

df1.writeStream.option("path",f"{outputpath}/reshma/teststream/files").option("checkpointLocation",f"{outputpath}/reshma/teststream/checkpoint").trigger(processingTime="1 minute").toTable("stream.teststream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.teststream

# COMMAND ----------

for i in spark.streams.active:
    i.stop()

# COMMAND ----------


