# Databricks notebook source
# MAGIC %md
# MAGIC ## カタログ名とスキーマ名を定義

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
catalog_name = dbutils.widgets.get("catalog_name")
print(f"catalog_name is {catalog_name}")

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")
print(f"schema_name is {schema_name}")

dbutils.widgets.text("sample_dataset_path", "")
sample_dataset_path = dbutils.widgets.get("sample_dataset_path")
print(f"sample_dataset_path is {sample_dataset_path}")

dbutils.widgets.text("table_name_prefix", "")
tgt_table_name_prefix = dbutils.widgets.get("table_name_prefix")
print(f"tgt_table_name_prefix is {tgt_table_name_prefix}")

# COMMAND ----------

import os
parent_dir = os.path.dirname(os.getcwd())
src_dir = f"{sample_dataset_path}/sfa"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 共通処理

# COMMAND ----------

import os

from pyspark.sql.functions import expr

# COMMAND ----------

current_dir = os.getcwd()

# COMMAND ----------

def load_casted_data_to_spark_table(
    src_file_path,
    tgt_catalog_name,
    tgt_schema_name,
    tgt_table_name,
    write_method="overwrite",
    drop_cols=[],
):
    tgt_table_full_name = f"{tgt_catalog_name}.{tgt_schema_name}.{tgt_table_name}"
    src_df = (
        spark.read.format("csv")
        .option("header", True)
        .option("multiLine", True)
        .load(src_file_path)
    )

    if drop_cols != []:
        src_df = src_df.drop(*drop_cols)

    # 書き込み先テーブルのデータ型に変換
    tgt_df = spark.table(tgt_table_full_name)
    tgt_df = tgt_df.select(src_df.columns)
    col_maps = {}
    for col_name, col_type in tgt_df.dtypes:
        if col_type == "date":
            col_maps[col_name] = expr(f"to_date(`{col_name}`)")
        if col_type == "timestamp":
            col_maps[col_name] = expr(f"to_timestamp(`{col_name}`)")
        else:
            col_maps[col_name] = src_df[col_name].cast(col_type)
    if col_maps != {}:
        src_df = src_df.withColumns(col_maps)
    print(f"-- {tgt_table_name}: {src_df.count()}")
    src_df.limit(50).display()

    src_df.write.mode(write_method).saveAsTable(tgt_table_full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account (取引先)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "account"
src_file_path = f"{src_dir}/Account.csv"

drop_cols = [
    # 下記は Data Manger でのみ付与されるカラムであるため DROP
    "Segment__c",
]

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
    drop_cols=drop_cols,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract (契約)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "contact"
src_file_path = f"{src_dir}/Contact.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lead (リード)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "lead"
src_file_path = f"{src_dir}/Lead.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Campaign (キャンペーン)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "campaign"
src_file_path = f"{src_dir}/Campaign.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opportunity (商談)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "opportunity"
src_file_path = f"{src_dir}/Opportunity.csv"

drop_cols = [
    # 下記は Data Manger でのみ付与されるカラムであるため DROP
    "Segment__c",
    "Opportunity_Source__c",
    "RecordTypeId",
    "Amount_CAD__c",
]

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
    drop_cols=drop_cols,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product (製品)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "product2"
src_file_path = f"{src_dir}/Product2.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Book Entry (価格表エントリ)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "pricebook_entry"
src_file_path = f"{src_dir}/PricebookEntry.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Case (ケース)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "case"
src_file_path = f"{src_dir}/Case.csv"

drop_cols = [
    # 下記は Data Manger でのみ付与されるカラムであるため DROP
    "CSAT__c",
    "Case_ExternalId__c",
    "FCR__c",
    "Product_Family_KB__c",
    "SLA_Type__c",
]

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
    drop_cols=drop_cols,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## User（ユーザ）

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "user"
src_file_path = f"{src_dir}/User.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order (注文)

# COMMAND ----------

tgt_table_name = tgt_table_name_prefix + "order"
src_file_path = f"{src_dir}/Order.csv"

# COMMAND ----------

load_casted_data_to_spark_table(
    src_file_path=src_file_path,
    tgt_catalog_name=catalog_name,
    tgt_schema_name=schema_name,
    tgt_table_name=tgt_table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
