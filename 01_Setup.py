# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. 表示言語の設定

# COMMAND ----------

# MAGIC %md
# MAGIC Settings から任意の言語（例：日本語）を選択してください。
# MAGIC </br><img src="images/setup.1.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ハンズオンコンテンツのセットアップ

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオンコンテンツのダウンロード

# COMMAND ----------

# MAGIC %md
# MAGIC #### 任意：ハンズオンコンテンツの最新化
# MAGIC [リポジトリ](https://github.com/gho9o9/adb-handson) から最新版を入手（PULL）してください。
# MAGIC </br><img src="images/readme.6.png" width="600"/>
# MAGIC </br><img src="images/readme.7.png" width="600"/>
# MAGIC </br><img src="images/readme.8.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオンコンテンツの環境設定

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Databricks クラスタの準備

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
# MAGIC ## 4. サンプルデータのセットアップ

# COMMAND ----------

# MAGIC %md
# MAGIC #### クラスタのアタッチ

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオン環境用おまじない

# COMMAND ----------

# MAGIC %run ./include/handson.h

# COMMAND ----------

# MAGIC %md
# MAGIC #### Volume の作成（Databricks Runtime 13.3 LTS and above）

# COMMAND ----------

# spark.sql(f"""DROP VOLUME IF EXISTS {your_volume}""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {your_volume}""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Volume へのアップロード（マニュアル操作）
# MAGIC 1. `sample_dataset` フォルダのハンバーガーメニュー（`︙`）を選択して、 `エクスポート` -> `ソースファイル` をクリックしてデータファイルをダウンロードします。
# MAGIC 1. ダウンロードした Zip ファイルを解凍します。
# MAGIC 1. ノートブックの左型タブにある`Catalog (Ctrl + Alt + C)`を選択したのち、`sample_dataset_volume` ボリュームのハンバーガーメニュー（`︙`）から`ボリュームへのアップロード`を選択します。
# MAGIC </br><img src="images/00.2.1.png" width="600"/>
# MAGIC 1. 表示されたウィンドウに解凍した `sample_dataset` フォルダ配下のすべてのフォルダ（全7フォルダ）をドロップし`アップロード`を押下します。
# MAGIC </br><img src="images/00.2.2.png" width="600"/>
# MAGIC </br><img src="images/00.2.3.png" width="600"/>
# MAGIC </br><img src="images/00.2.4.png" width="600"/>
