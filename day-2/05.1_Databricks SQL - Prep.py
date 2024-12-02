# Databricks notebook source
# MAGIC %md
# MAGIC ## コンテキスト設定

# COMMAND ----------

# MAGIC %run ./00.1_Set_Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## サンプルデータ確認

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`

# COMMAND ----------

# MAGIC %md
# MAGIC ## エンリッチ

# COMMAND ----------

# MAGIC %pip install geopy

# COMMAND ----------

from geopy.geocoders import Nominatim

def get_location_name(latitude, longitude, scope, lang):
    geolocator = Nominatim(user_agent="tomo@o9o9.cloud")
    location = geolocator.reverse((latitude, longitude), language=lang)
    # return location.address
    # return location.raw
    return location.raw['address'].get(scope, 'Unknown')

# UDFを登録
from pyspark.sql.types import StringType
get_location_name_udf = udf(get_location_name, StringType())

# latitude = 40.7143
# longitude = -74.0060
# location.raw['address']
# 'address': {'office': 'Sun Building', 'house_number': '280', 'road': 'Broadway', 'neighbourhood': 'Tribeca', 'suburb': 'Manhattan', 'county': 'New York County', 'city': 'New York', 'state': 'New York', 'ISO3166-2-lvl4': 'US-NY', 'postcode': '10007', 'country': 'United States', 'country_code': 'us'}

# 使用例
latitude = 40.7143 #35.6895
longitude = -74.0060 #139.6917
print(get_location_name(latitude, longitude, 'suburb', 'en'))
print(get_location_name(latitude, longitude, 'neighbourhood', 'en')+"/"+get_location_name(latitude, longitude, 'road', 'en'))

# COMMAND ----------

df = spark.sql("SELECT * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow` LIMIT 30")
display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date

# UDFを使って列を加工
df = df.withColumn("suburb", get_location_name_udf(df["pickup_latitude"], df["pickup_longitude"], lit('suburb'), lit('en')))
df = df.withColumn("neighbourhood", get_location_name_udf(df["pickup_latitude"], df["pickup_longitude"], lit('neighbourhood'), lit('en')))
df = df.withColumn("pickup_date", to_date(df["pickup_datetime"]))

# 結果の表示
# display(df.limit(10))

# COMMAND ----------

# df.createOrReplaceTempView('NYCTaxiSilverTable')
df.write.format("delta").mode("overwrite").saveAsTable("05_silver_nyctaxi")
