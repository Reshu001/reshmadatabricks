# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Spark(Spark Session):Spark is an entry point to start you Driver Program

# COMMAND ----------

users=[(1,'a',30),(2,'b',32)]

# COMMAND ----------

sampledf=spark.createDataFrame(users)

# COMMAND ----------

sampledf.show()

# COMMAND ----------

users=[(1,'a',30),(2,'b',32)]
users_schema_str= " id int, name string, age int"
df1=spark.createDataFrame(users,users_schema_str)
df1.display()

# COMMAND ----------


