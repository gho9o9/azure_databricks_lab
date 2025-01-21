# Databricks notebook source
files = dbutils.fs.ls("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow` limit 10
# MAGIC
# MAGIC select
# MAGIC   payment_type,
# MAGIC   pickup_datetime,
# MAGIC   passenger_count,
# MAGIC   trip_distance,
# MAGIC   tip_amount,
# MAGIC   fare_amount,
# MAGIC   total_amount
# MAGIC
# MAGIC count() TotalTripCount
# MAGIC sum(passenger_count) TotalPassengerCount
# MAGIC sum(trip_distance) TotalDistanceTravelled
# MAGIC sum(tip_amount) TotalTipAmount
# MAGIC sum(fare_amount) TotalFareAmount
# MAGIC sum(total_amount) TotalTripAmount
# MAGIC group by pickup_datetime, payment_type, 
# MAGIC
# MAGIC
# MAGIC

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


df = spark.sql("SELECT * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
display(df.limit(10))
#df.show(5)


# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
# UDFを登録
get_location_name_udf = udf(get_location_name, StringType())

# UDFを使って列を加工
df = df.withColumn("borough", get_location_name_udf(df["pickup_latitude"], df["pickup_longitude"], lit('suburb'), lit('en')))
df = df.withColumn("zone", get_location_name_udf(df["pickup_latitude"], df["pickup_longitude"], lit('neighbourhood'), lit('en')))

# 結果の表示
display(df.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o9o9dbw.handson_day2_tooota.get_location_name(pickup_latitude, pickup_longitude),* FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow` limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION o9o9dbw.handson_day2_tooota.get_location_name(latitude double, longitude double) RETURNS STRING
# MAGIC LANGUAGE PYTHON AS $$
# MAGIC def get_location_name(latitude, longitude):
# MAGIC     from geopy.geocoders import Nominatim
# MAGIC     geolocator = Nominatim(user_agent="tomo@o9o9.cloud")
# MAGIC     location = geolocator.reverse((latitude, longitude), language='en')
# MAGIC     return location.raw['address'].get('country', 'Unknown')
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o9o9dbw.handson_day2_tooota.get_location_name(35.6895, 139.6917)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION o9o9dbw.handson_day2_tooota.get_location_name() RETURNS STRING
# MAGIC LANGUAGE PYTHON AS $$
# MAGIC def get_location_name():
# MAGIC     from geopy.geocoders import Nominatim
# MAGIC     return 'aa'
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select o9o9dbw.handson_day2_tooota.get_location_name()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION o9o9dbw.handson_day2_tooota.hello() RETURNS STRING LANGUAGE PYTHON
# MAGIC  AS $$
# MAGIC def hello():
# MAGIC     return 'aa'
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o9o9dbw.handson_day2_tooota.hello() AS result;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION o9o9dbw.handson_day2_tooota.hello
