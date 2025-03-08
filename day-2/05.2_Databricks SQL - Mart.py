# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL
# MAGIC
# MAGIC Databricks SQL（DWH）を利用しデータ加工とデータ可視化を行います。
# MAGIC
# MAGIC このノートブックでは Silver テーブルを SQL で加工し Gold テーブル（いわゆるデータマート）を作成します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## はじめに
# MAGIC
# MAGIC このラボは プロビジョニング or サーバレス いずれかの SQL ウェアハウス を利用します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL ウェアハウス の作成
# MAGIC 1. サイドバーの **SQL ウェアハウス** をクリックします。  
# MAGIC 1. 右肩の**SQL ウェアハウスを作成**をクリックし以下を指定し SQL ウェアハウスを**作成**します。  
# MAGIC    - エンドポイント名：参加者全体で一意となるようあなたに固有の識別子を含めてください。
# MAGIC    * クラスターサイズ：ラボでは XXS を選択します。
# MAGIC    * 自動停止：ラボでは 10分 とします。
# MAGIC    * スケーリング：ラボでは最小、最大ともに 1 とします。
# MAGIC    * タイプ：ラボでは サーバレス とします。
# MAGIC </br><img src="../images/dwh.2.png" width="400"/>

# COMMAND ----------

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
# MAGIC   total_trip_amount double
# MAGIC );
# MAGIC
# MAGIC -- Silver 加工データを Gold にロード
# MAGIC INSERT INTO 05_gold_nyctaxi 
# MAGIC SELECT 
# MAGIC   pickup_date, 
# MAGIC   suburb, 
# MAGIC   neighbourhood, 
# MAGIC   payment_type,
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 注意：前のノートブックで Silver テーブル（05_silver_nyctaxi）が作成できなかった場合
# MAGIC
# MAGIC あらかじめ用意したデータファイルを使って Gold テーブル（05_gold_nyctaxi）を作成し次のノートブックに進んでください。

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

df = spark.read.parquet(f"{sample_dataset_path}/nyctaxi_yellow")
display(df.limit(10))


# COMMAND ----------

df.count()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("05_gold_nyctaxi")

# COMMAND ----------

# 内部メモ
# 
# %%sql
# 
# create database lakedb01
# 
# %%scala
# 
# val df = spark.read.sqlanalytics("sqlpool.adpe2e.TaxiDataSummary") 
# val df2 = df.withColumnRenamed("PickUpDate", "pickup_date").withColumnRenamed("PickUpBorough", "pickup_borough").withColumnRenamed("PickUpZone", "pickup_zone").withColumnRenamed("PaymentType", "payment_type").withColumnRenamed("TotalTripCount", "total_trip_count").withColumnRenamed("TotalPassengerCount", "total_passenger_count").withColumnRenamed("TotalDistanceTravelled", "total_trip_distance").withColumnRenamed("TotalTipAmount", "total_tip_amount").withColumnRenamed("TotalFareAmount", "total_fare_amount").withColumnRenamed("TotalTripAmount", "total_trip_amount")
# 
# import org.apache.spark.sql.types.DateType
# import org.apache.spark.sql.types.LongType
# import org.apache.spark.sql.types.DoubleType
# val df3 = df2.withColumn("pickup_date", df2("pickup_date").cast(DateType)).withColumn("total_trip_count", df2("total_trip_count").cast(LongType)).withColumn("total_passenger_count", df2("total_passenger_count").cast(LongType)).withColumn("total_trip_distance", df2("total_trip_distance").cast(DoubleType)).withColumn("total_tip_amount", df2("total_tip_amount").cast(DoubleType)).withColumn("total_fare_amount", df2("total_fare_amount").cast(DoubleType)).withColumn("total_trip_amount", df2("total_trip_amount").cast(DoubleType))
# 
# df3.write.format("delta").mode("overwrite").saveAsTable("lakedb01.05_gold_nyctaxi")
