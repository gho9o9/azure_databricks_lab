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

dbutils.widgets.text("table_name_prefix", "")
table_name_prefix = dbutils.widgets.get("table_name_prefix")
print(f"table_name_prefix is {table_name_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 共通処理

# COMMAND ----------

import inspect

def drop_table(table_name):
    table_name_w_prefix = f"{table_name_prefix}{table_name}"
    drop_state = "DROP TABLE IF EXISTS"
    drop_ddl = "{drop_state} {catalog_name}.{schema_name}.{table_name};"
    drop_ddl = inspect.cleandoc(drop_ddl)
    drop_ddl = drop_ddl.format(
        drop_state=drop_state,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name_w_prefix,
    )
    print(drop_ddl)
    return spark.sql(drop_ddl)

# COMMAND ----------

table_names = [
    "account",
    "contact",
    "lead",
    "campaign",
    "opportunity",
    "product2",
    "pricebook_entry",
    "case",
    "user",
    "order",
]
for table_name in table_names:
    drop_table(table_name)
