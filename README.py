# Databricks notebook source
# MAGIC %md
# MAGIC ## 事前確認事項
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 参加者権限設定
# MAGIC 一般ユーザーでハンズオンを行う場合は一般ユーザー既定の権限に対して以下の権限を追加してください。
# MAGIC - CREATE CATALOG 
# MAGIC - USE CATALOG 
# MAGIC - CREATE SCHEMA
# MAGIC - CREATE VOLUME
# MAGIC - CREATE CLUSTER（ユーザー管理画面で`Unrestricted cluster creation`にチェック）
# MAGIC </br><img src="images/readme.2.png" width="600"/>
# MAGIC </br><img src="images/readme.1.png" width="600"/>
# MAGIC </br><img src="images/readme.3.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### 必要ツール
# MAGIC - [Power BI Desktop](https://www.microsoft.com/ja-jp/power-platform/products/power-bi/desktop) を各自の PC にインストールしてください。最新版が望ましいです。
# MAGIC </br><img src="images/readme.4.png" width="600"/>
# MAGIC </br><img src="images/readme.5.png" width="600"/>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオンコンテンツ
# MAGIC [リポジトリ](https://github.com/gho9o9/adb-handson) から最新版を入手（PULL）してください。
# MAGIC </br><img src="images/readme.6.png" width="600"/>
# MAGIC </br><img src="images/readme.7.png" width="600"/>
# MAGIC </br><img src="images/readme.8.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Databricks クラスタ
# MAGIC 本ハンズオンでは以下のクラスタを作成します。
# MAGIC - 汎用クラスタ（Runtime 13.3 LTS 以上）
# MAGIC - ジョブクラスタ（ラボの過程で自動作成されます）
# MAGIC - SQL ウェアハウス
# MAGIC </br><img src="images/readme.9.png" width="600"/>
# MAGIC </br><img src="images/readme.10.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### リソース
# MAGIC - [ハンズオンコンテンツ（Github リポジトリ）](https://github.com/gho9o9/adb-handson)
