-- Databricks notebook source
use iotdata;
show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Standard View

-- COMMAND ----------

create view tempabove25 as (select * from sample where temperature>25)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Temp View

-- COMMAND ----------

create temp view tempabove25 as (select * from sample where temperature>25)

-- COMMAND ----------

show views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.CreateOrReplaceTempView("nametemp")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Global View

-- COMMAND ----------

create Global temp view tempabove25 as (select * from sample where temperature>25)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceGlobalTempView("namesglobal")

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from global_temp.namesglobal

-- COMMAND ----------


