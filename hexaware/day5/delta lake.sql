-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/sasdlsrs/bronze/delta/reshma/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ways to create delta table.
-- MAGIC 1.sql
-- MAGIC 2.python
-- MAGIC 3.df

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create schema delta;
-- MAGIC use delta

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS delta.people10m (  id INT,  firstName STRING,  middleName STRING,  lastName STRING,  gender STRING,  birthDate TIMESTAMP,  ssn STRING,  salary INT) 

-- COMMAND ----------

describe extended delta.people10m

-- COMMAND ----------

describe history delta.people10m

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS delta.people20m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) LOCATION 'dbfs:/mnt/sasdlsrs/bronze/delta/reshma'

-- COMMAND ----------

describe extended delta.people20m

-- COMMAND ----------

describe history delta.people20m

-- COMMAND ----------

insert into delta.people20m values(1,'Virat','R','K','M',2023-11-14,"123",1500)


-- COMMAND ----------


