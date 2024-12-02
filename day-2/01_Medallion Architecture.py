# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{sample_dataset}/checkpoints/orders_raw")
    .load(f"{sample_dataset}/orders-raw")
    .createOrReplaceTempView("01_raw_orders_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enriching Raw Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 01_raw_orders AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM 01_raw_orders_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_raw_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM 01_raw_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

(spark.table("01_raw_orders")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{sample_dataset}/checkpoints/orders_bronze")
      .outputMode("append")
      .table("01_bronze_orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM 01_bronze_orders

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating Static Lookup Table

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{sample_dataset}/customers-json")
      .createOrReplaceTempView("01_lookup_customers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_lookup_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

(spark.readStream
  .table("01_bronze_orders")
  .createOrReplaceTempView("01_bronze_orders_tmp"))

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

(spark.table("01_enriched_orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{sample_dataset}/checkpoints/orders_silver")
      .outputMode("append")
      .table("01_silver_orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM 01_silver_orders

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

(spark.readStream
  .table("01_silver_orders")
  .createOrReplaceTempView("01_silver_orders_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW 01_gold_daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM 01_silver_orders_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.table("01_gold_daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{sample_dataset}/checkpoints/daily_customer_books_gold")
      .trigger(availableNow=True)
      .table("01_gold_daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_gold_daily_customer_books

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
