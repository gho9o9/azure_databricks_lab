# Databricks notebook source
# MAGIC %md
# MAGIC ## サンプルデータのアップロード（必ずマニュアル操作が必要です）

# COMMAND ----------

# MAGIC %run ./00.1_Set_Environment

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
