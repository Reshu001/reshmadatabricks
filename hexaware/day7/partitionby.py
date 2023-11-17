# Databricks notebook source
df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("Year").count().orderBy("year").show()

# COMMAND ----------

df.write.delta
