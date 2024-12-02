-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Delta Live Tables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## DLT パイプライン設定と実行
-- MAGIC
-- MAGIC 1. サイドバーの **Delta Live Tables** をクリックします。
-- MAGIC 1. **パイプラインを作成**をクリックします。
-- MAGIC 1. **パイプライン名**を入力します。名称は参加者全体で一意となるようあなたに固有の識別子を含めてください。
-- MAGIC 1. **サーバレス**に`チェック`をします（ワークスペースでサーバレスが無効な場合はチェックを外し**製品エディション**は `Advanced` を選択します）。
-- MAGIC 1. **パイプラインモード**は `Trigger` を選択します。本ラボではファイルの取り込みを1回のみ行うため `Trigger` を選択しています。
-- MAGIC 1. **パス**はナビゲーターを使いこのノートブック（`02_Delta Live Tables`）選択します。
-- MAGIC 1. **ストレージオプション**は `Unity Catalog` を選択し、ラボで利用している `カタログ` と `スキーマ` を選択します。
-- MAGIC 1. **クラスターポリシー**は `なし` を選択し下記の 3 つを設定します。
-- MAGIC    * **クラスターモード**は `固定サイズ`を選択
-- MAGIC    * **ワーカ**は `1` を入力
-- MAGIC    * **Photonアクセラレータを使用**に `チェック`します。
-- MAGIC 1. **通知**で`設定を追加`を押下し下記の 3 つを設定します。
-- MAGIC    * **メール（カンマ区切り）**にメールアドレスを入力
-- MAGIC    * **更新時**をすべてチェック
-- MAGIC    * **フロー**をすべてチェック
-- MAGIC 1. **設定**で`設定を追加`を押下し下記の 2 つを設定します。
-- MAGIC    * **キー**に `sample.dataset` を入力し **値**に `00.1_Set_Environment`で定義された `sample_dataset`のパス文字列 を入力します。
-- MAGIC 1. **作成**を押下します。
-- MAGIC 1. **開始**を押下します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_raw

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 02_bronze_orders
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${sample.dataset}/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE 02_lookup_customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${sample.dataset}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC
-- MAGIC #### orders_cleaned

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 02_silver_orders (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.02_bronze_orders) o
  LEFT JOIN LIVE.02_lookup_customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE 02_gold_cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.02_silver_orders
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE 02_gold_fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.02_silver_orders
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
