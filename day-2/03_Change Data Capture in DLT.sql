-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## はじめに
-- MAGIC
-- MAGIC このラボはノートブック内のセルを順次実行していくようなインタラクティブな形式ではなく、以下の「DLT パイプライン設定と実行」の手順に従って DLT ジョブを構成＆実行します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## DLT パイプライン設定
-- MAGIC
-- MAGIC 1. サイドバーの **パイプライン** をクリックします。
-- MAGIC 1. 前のラボで作成したパイプラインを選択します。
-- MAGIC 1. `設定`を押下しパイプライン編集画面に遷移します。
-- MAGIC 1. **ソースコード**で`ソースコードを追加`を押下しナビゲーターを使いこのノートブック（`03_Change Data Capture in DLT`）選択します。
-- MAGIC 1. **保存**を押下します。
-- MAGIC 1. **開始**を押下します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_bronze_books

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 03_bronze_books -- ストリーム Read（増分取り込みを宣言）
COMMENT "The raw books data, ingested from CDC feed" -- コメント
AS SELECT * FROM cloud_files( -- Auto Loader 利用宣言（増分識別の機能有効化）
                             "${sample.dataset}/books-cdc", -- 入力元
                             "json") -- Foramat 指定

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Silver Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_silver_books

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 03_silver_books ( -- ストリーム Read（増分取り込みを宣言）
  CONSTRAINT valid_book_number EXPECT (book_id IS NOT NULL) ON VIOLATION DROP ROW -- 品質制約定義
);
-- CDF テーブルの増分データに対してロジックで差分処理を判定しターゲットテーブルを更新
APPLY CHANGES INTO LIVE.03_silver_books -- ターゲットテーブル
  FROM STREAM(LIVE.03_bronze_books) -- CDF テーブル
  KEYS (book_id) -- 比較キー
  APPLY AS DELETE WHEN row_status = "DELETE"  -- 増分データの条件に応じたターゲットへの振る舞いを定義
  SEQUENCE BY row_time -- トランザクションの順番判定キー
  COLUMNS * EXCEPT (row_status, row_time) -- ターゲットへ出力する列の指定（* EXCEPT で出力しない列指定も可能）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Gold Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_gold_author_counts_state

-- COMMAND ----------

CREATE LIVE TABLE 03_gold_author_counts_state -- マテリアライズドビュー（毎回洗い替え）
  COMMENT "Number of books per author" -- コメント
AS
  -- Books Gold テーブル用のデータ加工（分析用の集計処理）
   SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.03_silver_books
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Option. DLT View

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_books_sales_tmp

-- COMMAND ----------

CREATE LIVE VIEW 03_books_sales_tmp  -- 通常ビュー
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.02_silver_orders) o
    INNER JOIN LIVE.03_silver_books b
    ON o.book.book_id = b.book_id;
