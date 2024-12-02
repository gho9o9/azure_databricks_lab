# Databricks notebook source
your_catalog = "o9o9dbw" # 講師から提示されるカタログ名を入力してください
your_schema = "handson_day2_tooota" # 参加者全体で一意となるようあなたに固有の識別子をアルファベットで入力してください
data_source_uri = "abfss://o9o9dbw-catalog@o9o9stdbwcatalog.dfs.core.windows.net/bookstore" # 講師から提示されるパスを入力してください
sample_dataset = 'dbfs:/mnt/adb-handson-day2/bookstore'
spark.conf.set(f"sample.dataset", sample_dataset) # for SQL Context

# COMMAND ----------

def set_current_context(catalog_name, schema_name):
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    spark.sql(f"USE DATABASE {schema_name}")

# COMMAND ----------

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def download_dataset(source, target):
    files = dbutils.fs.ls(source)

    for f in files:
        source_path = f"{source}/{f.name}"
        target_path = f"{target}/{f.name}"
        if not path_exists(target_path):
            print(f"Copying {f.name} ...")
            dbutils.fs.cp(source_path, target_path, True)

# COMMAND ----------

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{sample_dataset}/orders-streaming"
raw_dir = f"{sample_dataset}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# DLT
streaming_orders_dir = f"{sample_dataset}/orders-json-streaming"
streaming_books_dir = f"{sample_dataset}/books-streaming"

raw_orders_dir = f"{sample_dataset}/orders-json-raw"
raw_books_dir = f"{sample_dataset}/books-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

download_dataset(data_source_uri, sample_dataset)
set_current_context(your_catalog, your_schema)
print("Setup completed.")
