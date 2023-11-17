-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS delta.people20m (
-- MAGIC   id INT,
-- MAGIC   firstName STRING,
-- MAGIC   middleName STRING,
-- MAGIC   lastName STRING,
-- MAGIC   gender STRING,
-- MAGIC   birthDate TIMESTAMP,
-- MAGIC   ssn STRING,
-- MAGIC   salary INT
-- MAGIC ) LOCATION 'dbfs:/mnt/sasdlsrs/bronze/delta/reshma'

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/sasdlsrs/bronze/delta/reshma/

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use delta;
-- MAGIC select * from delta.people20m

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into delta.people20m values(1,'Virat','R','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into delta.people20m values(2,'Rohit','R','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

describe history delta.people20m

-- COMMAND ----------

create table delta.people20mv3 as (select * from delta.people20m)

-- COMMAND ----------

select * from delta.people20m timestamp as  of '2023-11-15T08:55:22Z'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into delta.people20m values(3,'K','L','Rahul','M',2023-11-14,"123",1500)

-- COMMAND ----------


