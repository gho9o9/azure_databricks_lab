# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL
# MAGIC
# MAGIC Databricks SQL（DWH）を利用しデータ加工とデータ可視化を行います。
# MAGIC
# MAGIC このノートブックでは Silver テーブルの準備を行います。

# COMMAND ----------

# MAGIC %md
# MAGIC ## はじめに
# MAGIC
# MAGIC このラボは プロビジョニング or サーバレス いずれかの汎用クラスタを利用します。

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

# MAGIC %md
# MAGIC ## サンプルデータ確認
# MAGIC Databricks ワークスペース内に様々なサンプルデータがあらかじめ登録されています。
# MAGIC ここでは New York Taxi データセットを利用します。

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC およそ 16 億 レコードのデータセットです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver テーブルの準備（データエンリッチ）
# MAGIC ここでは New York Taxi データセット内の 緯度・経度 のデータを元にライブラリを利用し地名を導出し列に追加します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 元データ確認

# COMMAND ----------

df = spark.sql("SELECT * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow` LIMIT 100")
display(df.limit(10))

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 地名導出のためのライブラリをインストール

# COMMAND ----------

# MAGIC %pip install geopy

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDF 定義
# MAGIC
# MAGIC 4行目の user_agent に任意のメアドを設定ください。

# COMMAND ----------

from geopy.geocoders import Nominatim

def get_location_name(latitude, longitude, scope, lang):
    geolocator = Nominatim(user_agent="任意のメアドを設定ください")
    location = geolocator.reverse((latitude, longitude), language=lang)
    # return location.address
    # return location.raw
    return location.raw['address'].get(scope, 'Unknown')

# UDFを登録
from pyspark.sql.types import StringType
get_location_name_udf = udf(get_location_name, StringType())

# COMMAND ----------

# 使用例
latitude = 40.7143 #35.6895
longitude = -74.0060 #139.6917
print(get_location_name(latitude, longitude, 'suburb', 'en'))
print(get_location_name(latitude, longitude, 'neighbourhood', 'en')+"/"+get_location_name(latitude, longitude, 'road', 'en'))

# latitude = 40.7143
# longitude = -74.0060
# location.raw['address']
# 'address': {'office': 'Sun Building', 'house_number': '280', 'road': 'Broadway', 'neighbourhood': 'Tribeca', 'suburb': 'Manhattan', 'county': 'New York County', 'city': 'New York', 'state': 'New York', 'ISO3166-2-lvl4': 'US-NY', 'postcode': '10007', 'country': 'United States', 'country_code': 'us'}

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDF を利用し地名列を追加

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date

# UDFを使って列を加工
df = df.withColumn("suburb", get_location_name_udf(df["pickup_latitude"], df["pickup_longitude"], lit('suburb'), lit('en')))
df = df.withColumn("neighbourhood", get_location_name_udf(df["pickup_latitude"], df["pickup_longitude"], lit('neighbourhood'), lit('en')))
df = df.withColumn("pickup_date", to_date(df["pickup_datetime"]))

# 結果の表示
# df.cache()
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Table として書き出し
# MAGIC
# MAGIC **注意**：geopy のスロットリングによりエラーなる場合はエラーのまま次のノートブックに進んでください。

# COMMAND ----------

# df.createOrReplaceTempView('NYCTaxiSilverTable')
df.write.format("delta").mode("overwrite").saveAsTable("05_silver_nyctaxi")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM 05_silver_nyctaxi;
