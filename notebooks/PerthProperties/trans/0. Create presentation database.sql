-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create pp1_presentation database to keep gold tables

-- COMMAND ----------

Create Database if not exists pp1_presentation
Location "/mnt/perthpropdl/presentation"

-- COMMAND ----------

Desc database Extended pp1_presentation;

-- COMMAND ----------

