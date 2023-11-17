# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/formula/pit_stops.json",multiLine=True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------


