-- Databricks notebook source
-- MAGIC %python
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DeltaTable.createOrReplace(spark)\
-- MAGIC     .tableName("delta.employee")\
-- MAGIC     .addColumn("emp_id","int")\
-- MAGIC     .addColumn("emp_name","String")\
-- MAGIC     .addColumn("gender","String")\
-- MAGIC     .location("dbfs:/mnt/sasdlsrs/bronze/delta/emp")\
-- MAGIC     .execute()

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from  delta.employee

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into delta.employee values(1,"Sachin","M")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=[(2,'Rohit',"M")]
-- MAGIC schema=StructType([StructField("emp_id",IntegerType()),
-- MAGIC                    StructField("emp_name",StringType()),
-- MAGIC                    StructField("gender",StringType()),
-- MAGIC ])
-- MAGIC (spark.createDataFrame(data,schema).write.mode("append").saveAsTable("delta.employee"))

-- COMMAND ----------

select * from delta.employee

-- COMMAND ----------

describe history delta.employee

-- COMMAND ----------

select * from delta.employee

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=[(2,'Rohit',"M")]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.table("delta.employee")
-- MAGIC df.dropDuplicates()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=[(3,'virat',"M","Batsman")]
-- MAGIC schema=StructType([StructField("emp_id",IntegerType()),
-- MAGIC                    StructField("emp_name",StringType()),
-- MAGIC                    StructField("gender",StringType()),
-- MAGIC                    StructField("dept",StringType())
-- MAGIC ])
-- MAGIC (spark.createDataFrame(data,schema).write.mode("append").option("mergeSchema","true").saveAsTable("delta.employee"))

-- COMMAND ----------

select * from delta.employee

-- COMMAND ----------


