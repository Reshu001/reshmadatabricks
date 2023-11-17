# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

df_final=(df.withColumn("ingestiondate",current_date())
.withColumn("path",input_file_name())
.drop("url"))

# COMMAND ----------

df_final.write.saveAsTable("formula1.constructors")

# COMMAND ----------


