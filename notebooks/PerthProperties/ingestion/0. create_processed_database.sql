-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create pp1_processed database to keep silver tables

-- COMMAND ----------

Create Database If not Exists pp1_processed
Location "/mnt/perthpropdl/processed"

-- COMMAND ----------

Desc Database pp1_processed;

-- COMMAND ----------

