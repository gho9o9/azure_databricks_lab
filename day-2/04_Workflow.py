# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## ワークフロー設定
# MAGIC
# MAGIC 1. サイドバーの **ワークフロー** をクリックします。
# MAGIC 1. **ジョブを作成**をクリックします。
# MAGIC 1. **ジョブ名**を入力します。名称は参加者全体で一意となるようあなたに固有の識別子を含めてください。
# MAGIC 1. 以下の手順で`ノートブック タスク`を設定します。
# MAGIC    1. **タスク名**：`Ingest_Data`
# MAGIC    1. **種類**：`ノートブック`
# MAGIC    1. **ソース**：`ワークスペース`
# MAGIC    1. **パス**：ナビゲーターを使いこのノートブック（`04_Workflow`）選択
# MAGIC    1. **クラスター**：`Job＿Cluster`（`新しいジョブクラスターを追加`により任意のスペックを指定可能）
# MAGIC    1. `タスクを作成`を押下
# MAGIC 1. 以下の手順で`DLT タスク`を設定します。
# MAGIC    1. `Ingest_Data` タスクの下の`タスクを追加`を押下し`Delta Live Tables パイプライン`を選択
# MAGIC    1. **タスク名**：`Transform_Data_in_DLT`
# MAGIC    1. **パイプライン**：前のラボで作成した DLT パイプラインを選択
# MAGIC    1. `タスクを作成`を押下
# MAGIC 1. 必要に応じて**ジョブ通知**を設定します。
# MAGIC    * **メール（カンマ区切り）**にメールアドレスを入力
# MAGIC    * **更新時**をすべてチェック
# MAGIC    * **フロー**をすべてチェック
# MAGIC 1. **設定**で`設定を追加`を押下し下記の 2 つを設定します。
# MAGIC    * **キー**に `mypipeline.catalog_name` を入力し **値**にラボで利用している `カタログ` を入力
# MAGIC    * **キー**に `mypipeline.schema_name` を入力し **値**にラボで利用している `スキーマ` を入力
# MAGIC 1. **作成**を押下します。
# MAGIC 1. **今すぐ実行**を押下します。**スケジュールとトリガー**によってスケジュール実行やファイル到着実行を構成することも可能です。

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset}/orders-json-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * from json.`${sample.dataset}/orders-json-streaming/05.json` where customer_id = "C00788"

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset}/books-cdc")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * from json.`${sample.dataset}/books-cdc/05.json`
