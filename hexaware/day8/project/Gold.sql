-- Databricks notebook source
use project;

-- COMMAND ----------

select * from silver

-- COMMAND ----------

Create  or Replace table project.gold as (select product_name,sum(quantity) as totalquantity from silver group by all order by totalquantity desc)

-- COMMAND ----------

select * from project.gold

-- COMMAND ----------


