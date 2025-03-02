# Databricks notebook source
# MAGIC %md
# MAGIC # はじめに

# COMMAND ----------

# MAGIC %md
# MAGIC ラボコンテンツ（このノートブック）の実行に必要な Spark クラスタとして汎用コンピューティングクラスタを準備しノートブックにアタッチします。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 汎用コンピューティングクラスタの準備

# COMMAND ----------

# MAGIC %md
# MAGIC プロビジョニング or サーバレスで汎用コンピューティングクラスタを準備します。
# MAGIC
# MAGIC なおこのノートブックの実行において プロビジョニング or サーバレス は不問です。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロビジョニング 汎用コンピューティング クラスタ
# MAGIC
# MAGIC プロビジョニング汎用コンピューティングを利用する場合は `Databricks Runtime のバージョン` を `Runtime: 13.3 LTS` 以上を指定してください。
# MAGIC
# MAGIC またコストの観点からもハンズオンコンテンツの実行においては低スペックな`ノードタイプ（例：Standard_D4ads_v5）` かつ `シングルノード` で十分です。
# MAGIC
# MAGIC </br><img src="../images/basis.1.png" width="600"/>  
# MAGIC </br><img src="../images/basis.2.png" width="600"/>  

# COMMAND ----------

# MAGIC %md
# MAGIC ### サーバレス  汎用コンピューティング クラスタ
# MAGIC サーバレス汎用コンピューティングを利用する場合は明示的なデプロイは不要です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 汎用コンピューティングクラスタのアタッチ
# MAGIC
# MAGIC 準備した汎用コンピューティングクラスタをこのノートブックにアタッチします。
# MAGIC
# MAGIC ノートブックの右肩にあるクラスターリストから準備したクラスターを選択します。停止しているプロビジョニング汎用コンピューティングを選択した場合はそのタイミングで起動が開始されます。 
# MAGIC
# MAGIC </br><img src="../images/basis.3.png" width="600"/>  
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC # ノートブックの基本
# MAGIC
# MAGIC ここでは Databricks での開発における主要ツールとなる[ノートブック](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/)の基本を学習します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セルの実行
# MAGIC
# MAGIC セル内に記述された処理の実行はセルの左上にある `セルの実行` もしくは各種ショートカットキーを使用します。  
# MAGIC </br><img src="../images/basis.4.png" width="600"/>  

# COMMAND ----------

print("Hello Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブック言語の設定
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
# MAGIC ## マジックコマンド
# MAGIC ここでは代表的なマジックコマンドを紹介します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### %[lang] : 言語マジック
# MAGIC 言語マジックコマンドはノートブックのデフォルト言語以外の言語でコード実行する指示します。
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC * <strong><code>&#37;scala</code></strong>
# MAGIC * <strong><code>&#37;r</code></strong>
# MAGIC
# MAGIC ノートブックのデフォルト言語は主要言語を維持し他の言語でコードを実行する必要がある場合にのみ言語マジックを使用するのが推奨です。

# COMMAND ----------

# Python がデフォルト言語でマジックコマンドの指示なしに SQL を実行すると Python としてのシンタックスエラーとなります 

# SELECT "Hello Databricks SQL"; 

# COMMAND ----------

# MAGIC %sql -- Python がデフォルト言語で SQL を実行したい場合はマジックコマンドで言語を指定します 
# MAGIC SELECT "Hello Databricks SQL"; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### %md : Markdown
# MAGIC
# MAGIC マジックコマンド **&percnt;md** により、セル内でマークダウンを記述します。
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
# MAGIC ### %run : 別のノートブックのインライン実行
# MAGIC
# MAGIC **%run** マジックコマンドを使用してノートブックから別のノートブックを実行します。  
# MAGIC
# MAGIC 実行するノートブックは相対パスで指定します。  
# MAGIC
# MAGIC 別のノートブックはインラインで実行されるため別のノートブックで定義したオブジェクトは呼び出し元のノートブックから利用できます。  
# MAGIC
# MAGIC この仕組みによりノートブックをモジュール化できます。

# COMMAND ----------

# このノートブック内で定義されてない変数の参照はエラー

# print("sample_dataset_path = " + sample_dataset_path)

# COMMAND ----------

# MAGIC %run ../include/handson.h

# COMMAND ----------

# 呼び出した先のノートブック内で定義されている変数は参照可能
print("sample_dataset_path = " + sample_dataset_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### %pip : ライブラリのインストール
# MAGIC
# MAGIC [ノートブックスコープのライブラリ](https://learn.microsoft.com/ja-jp/azure/databricks/libraries/notebooks-python-libraries)は **%pip** を使用してインストールします。  
# MAGIC
# MAGIC **関連：**
# MAGIC - [クラスタースコープのライブラリのインストール](https://learn.microsoft.com/ja-jp/azure/databricks/libraries/cluster-libraries)
# MAGIC - [サーバーレスコンピュートへのライブラリのインストール](https://learn.microsoft.com/ja-jp/azure/databricks/compute/serverless/dependencies)
# MAGIC

# COMMAND ----------

# MAGIC %pip install matplotlib

# COMMAND ----------

import matplotlib.pyplot as plt

# X軸の定義
x = [1, 2, 3, 4, 5, 6]

# Y軸の定義
y1 = [100, 50, 150, 300, 100, 100 ]
y2 = [200, 300, 200, 400, 200, 300]
y3 = [500, 400, 300, 600, 100, 400]

# figureを生成
fig = plt.figure()

# axをfigureに設定
ax = fig.add_subplot(1, 1, 1)

# axに折れ線を設定
ax.plot(x, y1, "-", c="Blue", linewidth=1, marker='o', alpha=1)
ax.plot(x, y2, "-", c="Red", linewidth=1, marker='o', alpha=1)
ax.plot(x, y3, "-", c="Green", linewidth=1, marker='o', alpha=1)

# 表示
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### %sh : シェルコードの実行
# MAGIC
# MAGIC **%sh** を使用してシェル コードをノートブック内で実行します。シェル コードはドライバー上で処理されます。  

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /tmp

# COMMAND ----------

# MAGIC %md
# MAGIC シェル実行は[初期化スクリプト（クラスタ起動時に共通の環境変数、Spark 構成パラメータやライブラリ追加などを行う）](https://learn.microsoft.com/ja-jp/azure/databricks/init-scripts/)にて各ノードで実行するのが通常です。  
# MAGIC
# MAGIC 以下のサンプルは個別のシェル実行の一例です。  
# MAGIC
# MAGIC Databricks でのデータ保管先となる[オブジェクトストレージはファイルに対する追記や更新をサポートしない](https://learn.microsoft.com/ja-jp/azure/databricks/files/#volumes-limitations)ため、ローカルでファイル操作をした結果をシェル実行によりオブジェクトストレージコピーします。

# COMMAND ----------

import os
os.environ["object_storage"] = sample_dataset_path

# COMMAND ----------

# MAGIC %sh
# MAGIC # CSVファイル
# MAGIC output_file="sample.csv"
# MAGIC object_storage_path=$(printf "%s/%s" "$object_storage" "$output_file")
# MAGIC
# MAGIC # ヘッダー
# MAGIC echo "Name,Age,Location" > $object_storage_path
# MAGIC
# MAGIC # データ(オブジェクトストレージ上のファイルへの追記は不可「Operation not supported」)
# MAGIC echo "Alice,30,Tokyo" >> $object_storage_path
# MAGIC echo "Bob,25,Osaka" >> $object_storage_path
# MAGIC echo "Charlie,35,Nagoya" >> $object_storage_path
# MAGIC
# MAGIC cat $object_storage_path

# COMMAND ----------

# MAGIC %sh
# MAGIC # CSVファイル
# MAGIC output_file="sample.csv"
# MAGIC object_storage_path=$(printf "%s/%s" "$object_storage" "$output_file")
# MAGIC local_path="/tmp/${output_file}}"
# MAGIC
# MAGIC # ヘッダー
# MAGIC echo "Name,Age,Location" > $local_path
# MAGIC # データ
# MAGIC echo "Alice,30,Tokyo" >> $local_path
# MAGIC echo "Bob,25,Osaka" >> $local_path
# MAGIC echo "Charlie,35,Nagoya" >> $local_path
# MAGIC
# MAGIC # ローカルで加工したファイルをオブジェクトストレージへコピー
# MAGIC cp $local_path $object_storage_path
# MAGIC cat $object_storage_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## ファイル操作
# MAGIC [Databricks Utilities（dbutils）](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils) のファイル システム ユーティリティ（dbutils.fs.ls）を使用しファイルを操作します。 

# COMMAND ----------

# MAGIC %md
# MAGIC ### UC Volume の操作
# MAGIC dbutils.fs.ls("/Volumes/\<catalog-name\>/\<schema-name\>/\<volume-name\>/\<path\>")

# COMMAND ----------

volume_path = sample_dataset_path
print("volume_path = " + volume_path)

files = dbutils.fs.ls(volume_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ワークスペースファイルの操作（サーバーレスは未サポート）
# MAGIC dbutils.fs.ls("file:/Workspace/Users/\<user-name\>/\<path\>")

# COMMAND ----------

username = spark.sql("SELECT current_user()").collect()[0][0]
workspace_files_path = f"file:/Workspace/Users/{username}/"
print("workspace_files_path = " + workspace_files_path)

files = dbutils.fs.ls(workspace_files_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### クラウドストレージの操作
# MAGIC dbutils.fs.ls("abfss://\<container-name\>@\<storage-account-name\>.dfs.core.windows.net/\<path\>")

# COMMAND ----------

catalog_desc = spark.sql("DESCRIBE CATALOG EXTENDED o9o9dbw")

for row in catalog_desc.collect(): 
  if row["info_name"] == "Storage Root":
    cloud_storage_path = row["info_value"]
    break

print("cloud_storage_path = " + cloud_storage_path)
# 該当の外部ロケーションに対して READ FILES 権限が必要
files = dbutils.fs.ls(cloud_storage_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 参考
# MAGIC - [Azure Databricks 上のファイルを操作する](https://learn.microsoft.com/ja-jp/azure/databricks/files/)
# MAGIC - [Databricksのファイルシステムを可能な限りわかりやすく解説](https://qiita.com/taka_yayoi/items/075c6b3aeafac54c8ac4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブック間でのコード共有する
# MAGIC
# MAGIC ソースコードファイルをモジュールとしてノートブックにインポートすることでコードを共有できます。  
# MAGIC
# MAGIC #### 参考
# MAGIC - [Databricks ノートブック間でコードを共有する](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/share-code)

# COMMAND ----------

from package.demo import *
print(hello("databricks"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブックを通じた共同作業
# MAGIC ノートブックの右肩にある `共有` によって共同作業者へノートブックへのアクセス制御が可能です。
# MAGIC
# MAGIC | 能力                                      | 権限なし | 読み取り可能 | 実行可能 | 編集可能 | 管理可能 |
# MAGIC |-------------------------------------------|----------|--------------|----------|----------|----------|
# MAGIC | セルの表示                                |          | x            | x        | x        | x        |
# MAGIC | Comment (コメント)                        |          | x            | x        | x        | x        |
# MAGIC | %run またノートブック ワークフローを使用して実行する |          | x            | x        | x        | x        |
# MAGIC | ノートブックのアタッチとデタッチ          |          |              | x        | x        | x        |
# MAGIC | コマンドの実行                            |          |              | x        | x        | x        |
# MAGIC | セルの編集                                |          |              |          | x        | x        |
# MAGIC | アクセス許可の変更                        |          |              |          |          | x        |
# MAGIC
# MAGIC またコメントによって共同作業者とのディスカッションも可能です。
# MAGIC
# MAGIC #### 参考
# MAGIC - [Databricks ノートブックを使用して共同作業する](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/notebooks-collaborate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブックのインポート・エクスポート
# MAGIC ノートブックは次の形式でインポートやエクスポートが可能です。  
# MAGIC </br><img src="../images/basis.6.png" width="600"/>  
# MAGIC
# MAGIC | フォーマット | 解説 |
# MAGIC |----------|----------|
# MAGIC | DBCアーカイブ | 独自のアーカイブファイル |
# MAGIC | ソース ファイル | 拡張子が .scala、.py、.sql、または .r のソース コード ステートメントのみを含むファイル（フォルダの場合は ZIP ファイル） |
# MAGIC | HTML | 拡張子が .html の Azure Databricks ノートブック |
# MAGIC | R Markdown | 拡張子が の .Rmd の R マークダウンファイル |
# MAGIC | IPython | 拡張子が の .ipynb のノートブックファイル |
# MAGIC
# MAGIC #### 参考
# MAGIC - [Databricks ノートブックのエクスポートとインポート](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/notebook-export-import)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブックの UI パラメータ
# MAGIC パラメータを受け取る UI をノートブックに追加します。
# MAGIC
# MAGIC #### 参考
# MAGIC - [Databricks ウィジェット](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/widgets)
# MAGIC - [widgets ユーティリティ (dbutils.widgets)](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets)

# COMMAND ----------

# 下記のコード実行によりノートブックの上部にパラメータを指定する UI が追加されます。
dbutils.widgets.dropdown("state", "CA", ["CA", "IL", "MI", "NY", "OR", "VA"])

# COMMAND ----------

dbutils.widgets.get("state")

# COMMAND ----------

dbutils.widgets.remove("state")

# COMMAND ----------

# MAGIC %md
# MAGIC Jupyter Widgets（ipywidgets）を利用することでインタラクティブな実行をサポートすることもできます。
# MAGIC #### 参考
# MAGIC - [ipywidgets ウィジェット](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/ipywidgets)
# MAGIC - [Jupyter Widgets（ipywidgets）](https://ipywidgets.readthedocs.io/en/stable/)

# COMMAND ----------

import ipywidgets as widgets
from ipywidgets import interact

# Load a dataset
sparkDF = spark.read.csv("/databricks-datasets/bikeSharing/data-001/day.csv", header="true", inferSchema="true")

# In this code, `(bins=(3, 10)` defines an integer slider widget that allows values between 3 and 10.
@interact(bins=(3, 10))
def plot_histogram(bins):
  pdf = sparkDF.toPandas()
  pdf.hist(column='temp', bins=bins)

# COMMAND ----------

import ipywidgets as widgets

# Create button widget. Clicking this button loads a sampled dataframe from UC table.
button = widgets.Button(description="Load dataframe sample")

# Output widget to display the loaded dataframe
output = widgets.Output()

def load_sample_df(table_name):
  return spark.sql(f"SELECT * FROM {table_name} LIMIT 1000")

def on_button_clicked(_):
    with output:
      output.clear_output()
      df = load_sample_df('samples.tpch.region')
      print(df.toPandas())

# Register the button's callback function to query UC and display results to the output widget
button.on_click(on_button_clicked)

display(button, output)
