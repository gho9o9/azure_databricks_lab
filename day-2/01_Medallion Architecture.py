# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Orders Bronze Table の作成

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Orders Raw データを確認

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset}/orders-raw")
display(files)

# COMMAND ----------

df = spark.read.parquet(f"{sample_dataset}/orders-raw")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Auto Loader による Orders Raw データの 読み取り

# COMMAND ----------

(spark.readStream # ストリーム Read（増分取り込みを宣言）
    .format("cloudFiles") # Auto Loader 利用宣言（増分識別の機能有効化）
    .option("cloudFiles.format", "parquet") # Foramat 指定
    .option("cloudFiles.schemaLocation", f"{sample_dataset}/checkpoints/orders_raw") # スキーマ推論の有効化
    .load(f"{sample_dataset}/orders-raw") # 入力元
    .createOrReplaceTempView("01_raw_orders_temp")) # 一時ビュー作成（SQL による Raw データ加工用）

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Orders Bronze テーブル用のデータ加工（メタデータ付与）

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 01_raw_orders AS (
# MAGIC   SELECT *, 
# MAGIC     current_timestamp() arrival_time, 
# MAGIC     input_file_name() source_file
# MAGIC   FROM 01_raw_orders_temp
# MAGIC )
# MAGIC -- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_name.html

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ストリーム読み取り中のテーブルに対する読み取り
# MAGIC SELECT * FROM 01_raw_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ストリーム読み取り中のテーブルに対する読み取り
# MAGIC SELECT count(*) FROM 01_raw_orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Bronze Table への書き込み

# COMMAND ----------

(spark.table("01_raw_orders") # Bronze の入力元
      .writeStream # ストリーム読み取り中のデータ（読み取った増分データ）の出力を指示
      .format("delta") # 出力フォーマット
      .option("checkpointLocation", f"{sample_dataset}/checkpoints/orders_bronze") # 増分に対する Exactly-Once Ingest 保証のためのチェックポイント格納先
      .outputMode("append") # 読み取った増分データを追記することを指示
#     .trigger(processingTime='500 milliseconds' # 500 ms ごとの再実行を繰り返す（既定値）
      .table("01_bronze_orders")) # 出力先

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM 01_bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### テスト：新しい Orders Raw データの到着を疑似し上のストリームやマテリアライズドビューがどのように変化するか確認

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Orders Silver Table の作成

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Bronze テーブルの読み取り
# MAGIC Raw データの読み取りと比較し、入力元が Delta テーブルであるため、Auto Loader 利用宣言（増分識別の機能有効化）や スキーマ推論の有効化 は不要

# COMMAND ----------

(spark.readStream # ストリーム Read（増分取り込みを宣言）
# .format("cloudFiles") # Auto Loader 利用宣言（増分識別の機能有効化）
# .option("cloudFiles.format", "parquet") # Foramat 指定
# .option("cloudFiles.schemaLocation", f"{sample_dataset}/checkpoints/orders_raw") # スキーマ推論の有効化
  .table("01_bronze_orders") # Silver の入力元
  .createOrReplaceTempView("01_bronze_orders_tmp")) # 一時ビュー作成

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Customers マスターテーブルの読み取り

# COMMAND ----------

(spark.read # 通常 Read（全件読み取り）
      .format("json") # フォーマット指定
      .load(f"{sample_dataset}/customers-json") # 入力元
      .createOrReplaceTempView("01_lookup_customers")) # 一時ビュー作成

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_lookup_customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Silver テーブル用のデータ加工（Orders Bronze ストリーム と Customers 静的マスターテーブルとの結合）

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 01_enriched_orders_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM 01_bronze_orders_tmp o
# MAGIC   INNER JOIN 01_lookup_customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Silver テーブルへの書き込み

# COMMAND ----------

(spark.table("01_enriched_orders_tmp") # Silver の入力元
      .writeStream # ストリーム読み取り中のデータ（読み取った増分データ）の出力を指示
      .format("delta") # 出力フォーマット
      .option("checkpointLocation", f"{sample_dataset}/checkpoints/orders_silver") # 増分に対する Exactly-Once Ingest 保証のためのチェックポイント格納先
      .outputMode("append") # 読み取った増分データを追記することを指示
#     .trigger(processingTime='500 milliseconds' # 500 ms ごとの再実行を繰り返す（既定値）
      .table("01_silver_orders")) # 出力先

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM 01_silver_orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### テスト：新しい Orders Raw データの到着を疑似し上のストリームやマテリアライズドビューがどのように変化するか確認

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Orders Gold Table の作成

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Silver テーブルの読み取り
# MAGIC Raw データの読み取りと比較し、入力元が Delta テーブルであるため、Auto Loader 利用宣言（増分識別の機能有効化）や スキーマ推論の有効化 は不要

# COMMAND ----------

(spark.readStream # ストリーム Read（増分取り込みを宣言）
# .format("cloudFiles") # Auto Loader 利用宣言（増分識別の機能有効化）
# .option("cloudFiles.format", "parquet") # Foramat 指定
# .option("cloudFiles.schemaLocation", f"{sample_dataset}/checkpoints/orders_raw") # スキーマ推論の有効化
  .table("01_silver_orders") # 入力元
  .createOrReplaceTempView("01_silver_orders_tmp")) # 一時ビュー作成

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Gold テーブル用のデータ加工（分析用の集計処理）

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW 01_gold_daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM 01_silver_orders_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Gold テーブルへの書き込み
# MAGIC **outputMode("complete")** ： Bronze や Silver とは異なり Gold は集計処理のため追記ではなく全件を洗い替え（上書き）する  
# MAGIC **.trigger(availableNow=True)** : 増分読み取りを加味した再集計が完了したら処理を終了（既定は `.trigger(processingTime='500 milliseconds'`) で 500ms ごとの再実行を繰り返す）

# COMMAND ----------

(spark.table("01_gold_daily_customer_books_tmp") # Gold の入力元
      .writeStream # ストリーム読み取り中のデータ（読み取った増分データ）の出力を指示
      .format("delta") # 出力フォーマット
      .outputMode("complete") # 洗い替え指定（Bronze や Silver とは異なり Gold は集計処理のため追記ではなく全件上書き）
      .option("checkpointLocation", f"{sample_dataset}/checkpoints/daily_customer_books_gold") # 増分に対する Exactly-Once Ingest 保証のためのチェックポイント格納先
      .trigger(availableNow=True) # 増分読み取りが完了したら処理を終了（既定は処理を継続し新着入力をポーリング）
      .table("01_gold_daily_customer_books")) # 出力先

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_gold_daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
