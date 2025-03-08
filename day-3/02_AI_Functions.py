# Databricks notebook source
# MAGIC %md
# MAGIC # AI Functions による In-Database Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks には AI Functions として各種 AI 関数が提供されており、これを利用することで In-Database Analytics を実現できます。
# MAGIC
# MAGIC AI Functions は SQL を通じて AI モデルを簡単に利用できるように設計された機能群です。データエンジニアやデータサイエンティストは AI モデルを直接 SQL クエリ内で活用できるためデータ処理や分析のフローに AI 機能を統合できます。
# MAGIC
# MAGIC AI Functions が利用するモデルは組み込みのモデルのほか Azure Open AI など各種の外部モデルを利用できビジネス要件やプロジェクトのニーズに応じた柔軟なモデル選択が可能です。
# MAGIC
# MAGIC **参考**
# MAGIC - [Azure Databricks の AI 関数](https://learn.microsoft.com/ja-jp/azure/databricks/large-language-models/ai-functions)
# MAGIC - [Azure Databricks でのモデルの提供](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/model-serving/)
# MAGIC - [Mosaic AI Model Serving での外部モデル](https://learn.microsoft.com/ja-jp/azure/databricks/generative-ai/external-models/)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## はじめに
# MAGIC このラボは プロビジョニング or サーバレス いずれかの汎用クラスタを利用します。
# MAGIC
# MAGIC ここではサーバレスを利用しましょう。

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

model_serving_endpoint_name = f"gpt-4o-{your_identifier}"
print("model_serving_endpoint_name = " + model_serving_endpoint_name)

# COMMAND ----------

dbutils.widgets.text("your_catalog", your_catalog)
dbutils.widgets.text("your_schema", your_schema)
dbutils.widgets.text("model_serving_endpoint_name", model_serving_endpoint_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備
# MAGIC 事前準備として以下を行います。
# MAGIC - Azure Open AI GPT4-o のデプロイ
# MAGIC - サービングエンドポイントの設定
# MAGIC - データセットの準備

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure Open AI GPT4-o のデプロイ
# MAGIC 外部モデルとして利用する Azure Open AI GPT4-o をデプロイします。
# MAGIC
# MAGIC 1. Azure OpenAI をデプロイします。
# MAGIC </br><img src="../images/aoai.5.png" width="600"/>
# MAGIC 1. Azure OpenAI のデプロイが完了したらエンドポイントとキーをメモしておきます。
# MAGIC </br><img src="../images/aoai.11.png" width="600"/>
# MAGIC 1. 次に Azure AI Foundry Portal を開きます。
# MAGIC </br><img src="../images/aoai.6.png" width="600"/>
# MAGIC 1. サイドバーの`デプロイ`から`＋モデルのデプロイ`->`基本モデルをデプロイする`を選択します。
# MAGIC </br><img src="../images/aoai.7.png" width="600"/>
# MAGIC 1. 今回は gpt-4o を利用しましょう。モデルを選択したのち`確認`を押下します。
# MAGIC </br><img src="../images/aoai.8.png" width="600"/>
# MAGIC 1. ユニークな識別子を含む`デプロイ名`を入力したのち`デプロイ`を押下します。
# MAGIC </br><img src="../images/aoai.9.png" width="600"/>
# MAGIC 1. 必要に応じてレート制限を設定してください。
# MAGIC </br><img src="../images/aoai.10.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### サービングエンドポイントの設定
# MAGIC
# MAGIC 外部モデル（用意した Azure Open AI GPT4-o）を利用するモデルサービングエンドポイントを設定します。
# MAGIC
# MAGIC 1. サイドバーの**サービング**を選択し**サービングエンドポイントを作成する**ボタンをクリックします。
# MAGIC </br><img src="../images/aoai.12.png" width="1000"/>
# MAGIC 1. **名前**を入力します。名称は参加者全体で一意となるようあなたに固有の識別子を含めてください。
# MAGIC 1. **サービングエンティティ**の**Entity**のボックスをクリックし**サービングエンティティを選択**で `基盤モデル` と `OpenAI / Azure OpenAI`を選択し**確認**を押下します。
# MAGIC </br><img src="../images/aoai.1.png" width="1000"/>
# MAGIC 1. **OpenAIのAPIタイプ**は**Azure OpenAI**を選択します。
# MAGIC 1. **OpenAI APIキーシークレット**は用意した Azure OpenAI のキーを入力します。
# MAGIC 1. **OpenAIのAPIベース**は用意した Azure OpenAI のエンドポイントを入力します。
# MAGIC </br><img src="../images/aoai.2.png" width="1000"/>
# MAGIC 1. **OpenAIのデプロイメント名**は用意した gpt4-o のデプロイ名を入力します。
# MAGIC 1. [**OpenAIのAPIバージョン**](https://learn.microsoft.com/ja-jp/azure/ai-services/openai/api-version-deprecation)は現時点の最新 GA 版の **2024-10-21** を入力します。
# MAGIC 1. **タスク** は**Chat**を選択します。
# MAGIC 1. **プロバイダーモデル**は**gpt-4o**を選択します。
# MAGIC </br><img src="../images/aoai.3.png" width="1000"/>
# MAGIC 1. **AIゲートウェイ** の設定は任意とします。
# MAGIC </br><img src="../images/aoai.4.png" width="1000"/>
# MAGIC 1. **作成**をクリックします。

# COMMAND ----------

# MAGIC %md
# MAGIC ### データセットの準備
# MAGIC このラボで利用するデータセットを準備します。

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField("JAN", StringType(), True),
    StructField("商品名", StringType(), True),
    StructField("中身商品名", StringType(), True),
    StructField("発売日", StringType(), True)
])
df_bronze = spark.read.csv(f"{sample_dataset_path}/snack", header=True, inferSchema=True, schema=schema)
display(df_bronze.limit(10))

# COMMAND ----------

import pyspark.sql.functions as F

# 中身商品名が空ではないものを抽出
df_silver = (
    df_bronze
    .filter(F.col("中身商品名").isNotNull())
)

# データフレーム作成
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{your_catalog}.{your_schema}.07_silver_snack")
display(df_silver.limit(10))

# COMMAND ----------

df_silver.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Functions による商品の自動分類
# MAGIC
# MAGIC このラボでは **`ai_query()` 関数** 関数を利用し商品の自動分類を行います。
# MAGIC
# MAGIC
# MAGIC **`ai_query()` 関数**は AI Functions の一部で SQL クエリを通じてサービングエンドポイントで定義した外部モデルを利用します。この関数により以下のような応用が可能です。
# MAGIC
# MAGIC - テキスト解析、感情分析やデータクレンジングの自動化
# MAGIC - カスタムAIモデルへのクエリ
# MAGIC - データパイプライン内での高度なインサイト生成
# MAGIC
# MAGIC **参考**
# MAGIC - [ai_query() を使って提供されたモデルにクエリを実行する](https://learn.microsoft.com/ja-jp/azure/databricks/large-language-models/how-to-ai-query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC   ai_query('${model_serving_endpoint_name}',
# MAGIC     CONCAT('
# MAGIC         商品名に対して候補の中からいずれかひとつの適切な分類を判断し分類名のみを回答してください。適切な候補が無い場合は その他 と回答してください。
# MAGIC         - 商品名：',`中身商品名`,'
# MAGIC         - 分類名の候補：ジャガサラダ, カリカリえびせん, ポテトチップス'
# MAGIC     )
# MAGIC   ) AS `商品カテゴリー`
# MAGIC FROM
# MAGIC   ${your_catalog}.${your_schema}.07_silver_snack LIMIT 10; -- デモではデータを10件に絞ります

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CategorizedProducts AS (
# MAGIC   SELECT *,
# MAGIC     ai_query('${model_serving_endpoint_name}',
# MAGIC       CONCAT('
# MAGIC           商品名に対して候補の中からいずれかひとつの適切な分類を判断し分類名のみを回答してください。適切な候補が無い場合は その他 と回答してください。
# MAGIC           - 商品名：',`中身商品名`,'
# MAGIC           - 分類名の候補：ジャガサラダ, カリカリえびせん, ポテトチップス'
# MAGIC       )
# MAGIC     ) AS `商品カテゴリー`
# MAGIC   FROM
# MAGIC     ${your_catalog}.${your_schema}.07_silver_snack LIMIT 10 -- デモではデータを10件に絞ります
# MAGIC )
# MAGIC SELECT *,
# MAGIC   ai_query('${model_serving_endpoint_name}',
# MAGIC     CONCAT('
# MAGIC         ポテトチップス商品名に対して候補の中からいずれかひとつの味を判断し味の名称のみを回答してください。
# MAGIC         - ポテトチップス商品名：',`中身商品名`,'
# MAGIC         - 味の候補：コンソメ, うすしお, のりしお, 限定商品'
# MAGIC     )
# MAGIC   ) AS potato_category
# MAGIC FROM
# MAGIC   CategorizedProducts
# MAGIC where `商品カテゴリー` = 'ポテトチップス'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks 組み込みの基盤モデルを利用した AI Functions
# MAGIC
# MAGIC Databricks 組み込みの基盤モデルを利用した AI Functions には以下があります。
# MAGIC
# MAGIC - ai_analyze_sentiment
# MAGIC - ai_classify
# MAGIC - ai_extract
# MAGIC - ai_fix_grammar
# MAGIC - ai_gen
# MAGIC - ai_mask
# MAGIC - ai_similarity
# MAGIC - ai_summarize
# MAGIC - ai_translate
# MAGIC
# MAGIC 詳細：[Foundation Model API を使用して AI 関数を活用する](https://learn.microsoft.com/ja-jp/azure/databricks/large-language-models/ai-functions#ai-functions-using-foundation-model-apis)
# MAGIC
# MAGIC いくつか試してみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC 上でやったようなシンプルな分類であればが`ai_classify()`が利用できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH dataset AS (
# MAGIC   SELECT * FROM ${your_catalog}.${your_schema}.07_silver_snack ORDER BY JAN LIMIT 10 -- デモではデータを10件に絞ります
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   -- ai_classify(): 引数1に列名、引数2にラベリングする候補の配列を指定し、テキスト分類を行う
# MAGIC   ai_classify(`中身商品名`, array("ジャガサラダ", "カリカリえびせん", "ポテトチップス", "その他")) as `商品カテゴリー`
# MAGIC FROM
# MAGIC   dataset

# COMMAND ----------

# MAGIC %md
# MAGIC 他にもいくつか試してみましょう。

# COMMAND ----------

df = spark.read.format("parquet").option("header", True).load("/databricks-datasets/amazon/data20K/")
display(df.limit(10))
df.limit(10).createOrReplaceTempView("amazon_review")

# COMMAND ----------

# MAGIC %md
# MAGIC 上のレビューデータに対して`ai_translate()`による翻訳と`ai_analyze_sentiment()`感情分析をかけてみます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rating,
# MAGIC   ai_translate(review, "ja") review_ja,
# MAGIC   ai_analyze_sentiment(review) AS sentiment
# MAGIC FROM
# MAGIC   amazon_review;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 参考：UDF による実装
# MAGIC AI Functions と同様の AI 機能を UDF（ユーザー定義関数）で実装することも可能です。
# MAGIC
# MAGIC UDF はユーザーが独自にPython などで定義できる関数で DataFrame API に統合して利用できます。また SQL 関数を Python で実装することで SQL Warehouse にも統合が可能です。
# MAGIC
# MAGIC UDF により通常の組み込み関数では対応できないカスタマイズが必要なデータ操作が行えます。
# MAGIC
# MAGIC **参考**
# MAGIC - [生成AIを活用したテキスト分類/名寄せのアイデア【Databricks】](https://qiita.com/pepperland_sk/items/0cd9f8a3dece8eb67db3)

# COMMAND ----------

model_serving_endpoint_name = f"gpt-4o-{your_identifier}"

# COMMAND ----------

df_silver = spark.table(f"{your_catalog}.{your_schema}.07_silver_snack")

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルの実行前にコード内に変数を設定してください。
# MAGIC - `HOST`：Databricks ワークスペースのエンドポイント（例：https://adb-xxx.azuredatabricks.net）
# MAGIC - `DATABRICKS_TOKEN`：[個人用アクセストークン（PAT）](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/auth/pat)を指定してください。

# COMMAND ----------

from langchain.chat_models import ChatOpenAI
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# UDF 関数の定義
def llm_classifier_udf(query):
    DATABRICKS_TOKEN = "<YOUR_DATABRICKS_TOKEN>"
    HOST = 'https://adb-1450470117424213.13.azuredatabricks.net'

    try:
        # モデルの初期化を関数内で行う
        model = ChatOpenAI(
            model_name=f"{model_serving_endpoint_name}",
            openai_api_base=f"{HOST}/serving-endpoints/",
            openai_api_key=DATABRICKS_TOKEN
        )

        labels_list = ["ジャガサラダ", "カリカリえびせん", "ポテトチップス"]
        # ラベルの取得
        labels = labels_list

        # プロンプトの作成
        prompt_text = f"""
        入力されたワードに対して、ラベル候補に基づいてラベリングを行ってください。
        - 分類対象のワード: {query}
        - 分類を行うラベルの候補: {labels}
        - レスポンスのフォーマット: 以下に示す"label"と"confidence"のキーを持つJSONオブジェクトのみを返してください（```jsonの記述は不要です）。
        """

        # モデルを使用して予測を取得
        response = model.predict(prompt_text)
        # # JSON レスポンスをパース
        # res_dict = json.loads(response)
        # return res_dict.get('label', None)
        
        return response
    except Exception as e:
        return f"Error: {str(e)}"

# UDF の登録
llm_classifier_spark_udf = udf(llm_classifier_udf, StringType())

# COMMAND ----------

# テスト
data = [("ポテトチップスのりしお")]
columns = ["product"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)
res = llm_classifier_udf(pysparkDF[0])
print(res)

# COMMAND ----------

import pyspark.sql.functions as F

# データフレームへの適用
df_with_predictions = (
    df_silver.limit(10) # デモではデータを10件に絞ります
    .withColumn('predicted_label', llm_classifier_spark_udf(df_silver['中身商品名']))
    #.withColumn("clean_json", F.regexp_replace(F.col("predicted_label"), "```json\\n|```", ""))
)

display(df_with_predictions)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# JSONのスキーマを定義
json_schema = StructType([
    StructField("label", StringType(), True),
    StructField("confidence", StringType(), True)  # confidenceが数値の場合はDoubleType()に変更
])

# JSON文字列をStructTypeに変換
df_parsed = df_with_predictions.withColumn("json_data", F.from_json(F.col("predicted_label"), json_schema))

# json_dataのフィールドを個別の列として取得
df_result = df_parsed.select(
    F.col("商品名"),
    F.col("中身商品名"),
    F.col("json_data.label").alias("商品カテゴリ"),
    F.col("json_data.confidence").alias("信頼度")
)

display(df_result)
