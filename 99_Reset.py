# Databricks notebook source
# MAGIC %run ./00.1_Set_Environment

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset}")
display(files)

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {your_catalog}.{your_schema} CASCADE")
#dbutils.fs.rm(f"{sample_dataset}", True)
