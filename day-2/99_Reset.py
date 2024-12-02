# Databricks notebook source
your_catalog = "o9o9dbw" # 講師から提示されるカタログ名を入力してください
your_schema = "handson_day2_tooota" # 参加者全体で一意となるようあなたに固有の識別子をアルファベットで入力してください
sample_dataset = 'dbfs:/mnt/adb-handson-day2/bookstore'

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset}")
display(files)

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {your_catalog}.{your_schema} CASCADE")
dbutils.fs.rm(f"{sample_dataset}", True)

# COMMAND ----------

# dbutils.fs.rm(f"{sample_dataset}/checkpoints", True)

# dbutils.fs.cp(f"{sample_dataset}/orders-raw/01.parquet", f"{sample_dataset}/orders-raw-keep/01.parquet")
# dbutils.fs.rm(f"{sample_dataset}/orders-raw", True)
# dbutils.fs.mv(f"{sample_dataset}/orders-raw-keep/", f"{sample_dataset}/orders-raw/", True)

# dbutils.fs.cp(f"{sample_dataset}/orders-json-raw/01.json", f"{sample_dataset}/orders-json-raw-keep/01.json")
# dbutils.fs.rm(f"{sample_dataset}/orders-json-raw", True)
# dbutils.fs.mv(f"{sample_dataset}/orders-json-raw-keep/", f"{sample_dataset}/orders-json-raw/", True)

# dbutils.fs.cp(f"{sample_dataset}/books-cdc/01.json", f"{sample_dataset}/books-cdc-keep/01.json")
# dbutils.fs.rm(f"{sample_dataset}/books-cdc", True)
# dbutils.fs.mv(f"{sample_dataset}/books-cdc-keep/", f"{sample_dataset}/books-cdc/", True)
