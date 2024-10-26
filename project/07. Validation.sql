-- Databricks notebook source
SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_traffic;


-- COMMAND ----------

SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_roads


-- COMMAND ----------

SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_traffic


-- COMMAND ----------

SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_roads


-- COMMAND ----------

SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_traffic


-- COMMAND ----------

SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_roads
