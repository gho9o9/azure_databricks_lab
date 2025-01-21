# Databricks notebook source
# MAGIC %md
# MAGIC ## 汎用クラスターの準備

# COMMAND ----------

# MAGIC %md
# MAGIC ハンズオンコンテンツ（ノートブック）の実行に必要な汎用クラスターを準備します。デプロイモデル(プロビジョニング or サーバレス)は不問です。
# MAGIC
# MAGIC - プロビジョニング  
# MAGIC </br><img src="../images/basis.1.png" width="600"/>  
# MAGIC </br><img src="../images/basis.2.png" width="600"/>  
# MAGIC プロビジョニング汎用コンピューティングを利用する場合は `Databricks Runtime のバージョン` を `Runtime: 13.3 LTS` 以上を指定してください。  
# MAGIC またコストの観点からもハンズオンコンテンツの実行においては低スペックな`ノードタイプ（例：Standard_D4ads_v5）` かつ `シングルノード` で十分です。  
# MAGIC
# MAGIC - サーバレス  
# MAGIC サーバレス汎用コンピューティングを利用する場合は明示的なデプロイは不要です。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 汎用クラスターのアタッチ

# COMMAND ----------

# MAGIC %md
# MAGIC 処理の実行に必要なクラスターをアタッチします。  
# MAGIC </br><img src="../images/basis.3.png" width="600"/>  
# MAGIC ノートブックの右肩にあるクラスターリストから準備したクラスターを選択します。  
# MAGIC ※. 停止しているプロビジョニング汎用コンピューティングを選択した場合はそのタイミングで起動が開始されます。  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## セルの実行

# COMMAND ----------

print("Hello Databricks")
