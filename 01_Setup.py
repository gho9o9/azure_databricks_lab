# Databricks notebook source
# MAGIC %md
# MAGIC # ハンズオン事前準備

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 表示言語の設定

# COMMAND ----------

# MAGIC %md
# MAGIC 必要に応じて表示言語（既定：English）を変更します。  
# MAGIC </br><img src="images/setup.1.png" width="600"/>  
# MAGIC Settings から任意の言語（例：日本語）を選択してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ハンズオンコンテンツのセットアップ

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオンコンテンツのダウンロード
# MAGIC [GitHub リポジトリ](https://github.com/gho9o9/adb-handson)からハンズオンコンテンツをダウンロードします。  
# MAGIC </br><img src="images/setup.2.png" width="600"/>  
# MAGIC </br><img src="images/setup.3.png" width="600"/>  
# MAGIC `Git リポジトリの URL`に `https://github.com/gho9o9/adb-handson.git` を指定します。  
# MAGIC </br><img src="images/setup.4.png" width="600"/>  

# COMMAND ----------

# MAGIC %md
# MAGIC #### 任意：ハンズオンコンテンツの最新化
# MAGIC 既にクローン済みのリポジトリから最新のコンテンツをダウンロードする場合は PULL を行います。  
# MAGIC </br><img src="images/setup.5.png" width="600"/>  
# MAGIC </br><img src="images/setup.6.png" width="600"/>  

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオンコンテンツの環境設定
# MAGIC ハンズオンの環境設定を行います。  
# MAGIC </br><img src="images/setup.7.png" width="600"/>  
# MAGIC `includeフォルダ`内の`handson.h ノートブック`を開きます。  
# MAGIC </br><img src="images/setup.8.png" width="600"/>  
# MAGIC ノートブック冒頭のセル内で定義された変数（`your_catalog`と`your_schema`）の値を講師に指示に従い編集します。ここではセルを実行する必要はありません。  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. サンプルデータのセットアップ

# COMMAND ----------

# MAGIC %md
# MAGIC #### クラスターのアタッチ
# MAGIC サンプルデータのセットアップはノートブックの実行を伴うためクラスターをアタッチします。  
# MAGIC </br><img src="images/setup.9.png" width="600"/>  
# MAGIC このノートブックの右肩にあるクラスターリストから`サーバーレス`を選択します。`サーバーレス`が選択肢に無い場合は講師の指示に従います（汎用クラスタを別途作成してアタッチします）。

# COMMAND ----------

# MAGIC %md
# MAGIC #### ハンズオン用おまじない処理
# MAGIC ハンズオンのコンテンツを実行する場合に共通して事前実行するノートブックを呼び出します。
# MAGIC - `%run` によってノートブック内で別のノートブックをインラインで呼び出すことができます。
# MAGIC   - [Databricks ノートブックを別のノートブックから実行する](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/notebook-workflows)
# MAGIC - handson.h ノートブック内でハンズオンのコンテンツで使用するリソース（変数や関数）を定義しています。

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
# MAGIC #### Volume へのアップロード
# MAGIC サンプルデータを Volume へマニュアル操作でアップロードします。  
# MAGIC </br><img src="images/setup.10.png" width="600"/>  
# MAGIC `sample_dataset フォルダ`のハンバーガーメニュー（`︙`）を選択し`エクスポート` -> `ソースファイル` をクリックしダウンロードされた Zip ファイルを解凍しておきます。  
# MAGIC
# MAGIC </br><img src="images/00.2.1.png" width="600"/>  
# MAGIC ノートブックの左型タブにある`Catalog (Ctrl + Alt + C)`から`sample_dataset_volume ボリューム`を展開しハンバーガーメニュー（`︙`）の`ボリュームへのアップロード`を選択します。  
# MAGIC
# MAGIC </br><img src="images/00.2.2.png" width="600"/>  
# MAGIC </br><img src="images/00.2.3.png" width="600"/>  
# MAGIC 表示されたウィンドウに解凍した `sample_dataset` フォルダ配下のすべてのフォルダ（全7フォルダ）をドロップし`アップロード`を押下します。  
# MAGIC
# MAGIC </br><img src="images/00.2.4.png" width="600"/>  

# COMMAND ----------

# MAGIC %md
# MAGIC 以上で準備完了です。
# MAGIC
# MAGIC Next Action:
# MAGIC - <a href="$./day-1">Day-1 のハンズオンはこちら</a>
# MAGIC - <a href="$./day-2">Day-2 のハンズオンはこちら</a>
