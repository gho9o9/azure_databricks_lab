# Databricks notebook source
# MAGIC %md
# MAGIC ## SQL エディタ による SQL の実行
# MAGIC
# MAGIC 1. サイドバーの **SQL エディタ** をクリックします。
# MAGIC 1. エディタ中央上部のコンテキストメニュからラボで利用している `カタログ` と `スキーマ` を選択します。
# MAGIC </br><img src="../images/05.2.1.png" width="400"/>
# MAGIC 1. 以下のコードをエディタ内に張り付けステートメントごとに SQL を実行し動作を確認してください。
# MAGIC </br><img src="../images/05.2.2.png" width="400"/>
# MAGIC 1. サイドバーの **SQL カタログ** をクリックしラボで作成したテーブル（`05_silver_nyctaxi`、`05_gold_nyctaxi`）が作成されていることを確認します。
# MAGIC </br><img src="../images/05.2.3.png" width="400"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Gold テーブル定義
# MAGIC CREATE OR REPLACE TABLE 05_gold_nyctaxi(
# MAGIC   pickup_date date, 
# MAGIC   pickup_borough string,
# MAGIC   pickup_zone string,
# MAGIC   payment_type string,
# MAGIC   total_trip_count bigint,
# MAGIC   total_passenger_count bigint,
# MAGIC   total_trip_distance double,
# MAGIC   total_tip_amount double,
# MAGIC   total_fare_amount double,
# MAGIC   total_total_trip_amount double
# MAGIC );
# MAGIC
# MAGIC -- Silver 加工データを Gold にロード
# MAGIC INSERT INTO 05_gold_nyctaxi 
# MAGIC SELECT 
# MAGIC   pickup_date, suburb, neighbourhood, payment_type,
# MAGIC   count(*),
# MAGIC   sum(passenger_count),
# MAGIC   sum(trip_distance),
# MAGIC   sum(tip_amount),
# MAGIC   sum(fare_amount) + sum(extra) + sum(mta_tax),
# MAGIC   sum(total_amount)
# MAGIC FROM 05_silver_nyctaxi
# MAGIC GROUP BY pickup_date, payment_type, suburb , neighbourhood;
# MAGIC
# MAGIC SELECT * FROM 05_gold_nyctaxi LIMIT 10;
# MAGIC
# MAGIC -- 各種 DML
# MAGIC DELETE FROM 05_gold_nyctaxi WHERE total_tip_amount = 0;
# MAGIC UPDATE 05_gold_nyctaxi SET payment_type = "Credit Card" WHERE payment_type = "Credit";
# MAGIC DROP TABLE 05_gold_nyctaxi;
# MAGIC
# MAGIC -- CTAS で定義することも可能
# MAGIC CREATE OR REPLACE TABLE 05_gold_nyctaxi AS
# MAGIC SELECT 
# MAGIC   pickup_date, suburb as pickup_borough, neighbourhood as pickup_zone, payment_type,
# MAGIC   count(*) as total_trip_count,
# MAGIC   sum(passenger_count) as total_passenger_count,
# MAGIC   sum(trip_distance) as total_trip_distance,
# MAGIC   sum(tip_amount) as total_tip_amount,
# MAGIC   sum(fare_amount) as total_fare_amount,
# MAGIC   sum(total_amount) as total_total_amount
# MAGIC FROM 05_silver_nyctaxi
# MAGIC GROUP BY pickup_date, payment_type, suburb , neighbourhood;
# MAGIC
# MAGIC SELECT * FROM 05_gold_nyctaxi LIMIT 10;
# MAGIC ```
