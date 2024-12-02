-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## DLT パイプライン設定
-- MAGIC
-- MAGIC 1. サイドバーの **Delta Live Tables** をクリックします。
-- MAGIC 1. 前のラボで作成したパイプラインを選択します。
-- MAGIC 1. `設定`を押下しパイプライン編集画面に遷移します。
-- MAGIC 1. **ソースコード**で`ソースコードを追加`を押下しナビゲーターを使いこのノートブック（`03_Change Data Capture in DLT`）選択します。
-- MAGIC 1. **保存**を押下します。
-- MAGIC 1. **開始**を押下します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 03_bronze_books
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${sample.dataset}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 03_silver_books (
  CONSTRAINT valid_book_number EXPECT (book_id IS NOT NULL) ON VIOLATION DROP ROW
);
APPLY CHANGES INTO LIVE.03_silver_books
  FROM STREAM(LIVE.03_bronze_books)
  KEYS (book_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables

-- COMMAND ----------

CREATE LIVE TABLE 03_gold_author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.03_silver_books
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views

-- COMMAND ----------

CREATE LIVE VIEW 03_books_sales_tmp
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.02_silver_orders) o
    INNER JOIN LIVE.03_silver_books b
    ON o.book.book_id = b.book_id;
