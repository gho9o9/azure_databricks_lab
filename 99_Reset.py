# Databricks notebook source
# MAGIC %md
# MAGIC # ハンズオン環境の初期化

# COMMAND ----------

# MAGIC %md
# MAGIC ハンズオン環境の初期化としてユーザー固有のスキーマーを削除します。

# COMMAND ----------

# MAGIC %run ./include/handson.h

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset_path}")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS hive_metastore.${your_schema} CASCADE;

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {your_catalog}.{your_schema} CASCADE")
#dbutils.fs.rm(f"{sample_dataset}", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ハンズオン環境の初期化完了です。あらためてハンズオンを行う場合は `01_Setup` の `3. サンプルデータのセットアップ` から始めてください。
