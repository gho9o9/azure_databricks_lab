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
# MAGIC ## ノートブックの基本

# COMMAND ----------

# MAGIC %md
# MAGIC ### セルの実行
# MAGIC
# MAGIC セル内に記述された処理の実行はセルの左上にある `セルの実行` もしくは各種ショートカットキーを使用します。  
# MAGIC </br><img src="../images/basis.4.png" width="600"/>  

# COMMAND ----------

print("Hello Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ノートブック言語の設定
# MAGIC
# MAGIC 上記のセルはノートブックのデフォルト言語が Python に設定されているため Python コードとして実行されます。  
# MAGIC
# MAGIC Databricks のノートブックは Python、SQL、Scala、R がサポートされます。  
# MAGIC
# MAGIC デフォルト言語はノートブック左上のリストから選択・変更がいつでも可能です。  
# MAGIC
# MAGIC </br><img src="../images/basis.5.png" width="600"/>  

# COMMAND ----------

# MAGIC %md
# MAGIC ### マジックコマンド
# MAGIC ここでは代表的なマジックコマンドを紹介します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 言語マジック
# MAGIC 言語マジックコマンドはノートブックのデフォルト言語以外の言語でコード実行する指示します。
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC ノートブックのデフォルト言語は主要言語を維持し他の言語でコードを実行する必要がある場合にのみ言語マジックを使用するのが推奨です。

# COMMAND ----------

# Python がデフォルト言語でマジックコマンドの指示なしに SQL を実行すると Python としてのシンタックスエラーとなります 
SELECT "Hello Databricks SQL"; 

# COMMAND ----------

# Python がデフォルト言語で SQL を実行したい場合はマジックコマンドで言語を指定します 
%sql
SELECT "Hello Databricks SQL"; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Markdown
# MAGIC
# MAGIC マジックコマンド **&percnt;md** により、セル内でマークダウンを記述します
# MAGIC * このセルをダブルクリックして編集を開始します
# MAGIC * 編集を終了するには **`Esc`** キーを押します
# MAGIC
# MAGIC 以下は代表的な例です。
# MAGIC
# MAGIC # タイトル1
# MAGIC ## タイトル2
# MAGIC ### タイトル3
# MAGIC
# MAGIC これは **太字** の単語を含むテキストです。
# MAGIC
# MAGIC これは順序付きリストです
# MAGIC 1. 1つ
# MAGIC 1. 2つ
# MAGIC 1. 3つ
# MAGIC
# MAGIC これは順不同リストです
# MAGIC * りんご
# MAGIC * 桃
# MAGIC * バナナ
# MAGIC
# MAGIC リンク/埋め込みHTML： <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC
# MAGIC 画像：
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC テーブル：
# MAGIC
# MAGIC | 名前   | 値    |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC #### %run
# MAGIC
# MAGIC **%run** マジックコマンドを使用してノートブックから別のノートブックを実行できます。  
# MAGIC
# MAGIC 実行するノートブックは相対パスで指定します。  
# MAGIC
# MAGIC 別のノートブックはインラインで実行されるため別のノートブックで定義したオブジェクトは呼び出し元のノートブックから利用できます。  
# MAGIC
# MAGIC この仕組みによりノートブックをモジュール化できます。

# COMMAND ----------

# このノートブック内で定義されてない変数の参照はエラー
print("sample_dataset_path = " + sample_dataset_path)

# COMMAND ----------

# MAGIC %run ../include/handson.h

# COMMAND ----------

# 呼び出した先のノートブック内で定義されている変数は参照可能
print("sample_dataset_path = " + sample_dataset_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - %sh  
# MAGIC https://qiita.com/taka_yayoi/items/075c6b3aeafac54c8ac4  
# MAGIC https://learn.microsoft.com/ja-jp/azure/databricks/files/
# MAGIC - %fs  
# MAGIC https://qiita.com/taka_yayoi/items/075c6b3aeafac54c8ac4
# MAGIC https://learn.microsoft.com/ja-jp/azure/databricks/files/
# MAGIC
# MAGIC - %pip  
# MAGIC https://learn.microsoft.com/ja-jp/azure/databricks/libraries/notebooks-python-libraries
# MAGIC
# MAGIC - 全般  
# MAGIC https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/

# COMMAND ----------


dbutils.fs.ls(sample_dataset_path)

# COMMAND ----------

files = dbutils.fs.ls(sample_dataset_path)
display(files)

# COMMAND ----------

df.show()
display(df.limit(10))
