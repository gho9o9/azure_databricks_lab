# Databricks notebook source
# MAGIC %md
# MAGIC # AI/BI Genie を利用した自然言語によるデータ探索

# COMMAND ----------

# MAGIC %md
# MAGIC ## はじめに
# MAGIC このラボは プロビジョニング or サーバレス いずれかの汎用クラスタを利用します。
# MAGIC
# MAGIC ここではサーバレスを利用しましょう。

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

table_name_prefix = "06_"

# COMMAND ----------

# MAGIC %md
# MAGIC このラボで利用するテーブルを作成します。
# MAGIC
# MAGIC 処理の完了に 5 分程度要します（誤って再実行した場合は処理を途中でキャンセルしても問題ありません）。

# COMMAND ----------

init_notebooks = [
    "./include/01_create_tables",
    "./include/02_add_constraint",
    "./include/03_write_data",
]
notebook_parameters = {
    "catalog_name": your_catalog,
    "schema_name": your_schema,
    "sample_dataset_path": sample_dataset_path,
    "table_name_prefix": table_name_prefix,
}
for init_notebook in init_notebooks:
    dbutils.notebook.run(
        init_notebook,
        0,
        notebook_parameters,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie スペース の作成と基本動作

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie スペースの作成
# MAGIC サイドバーの **Genie** から右肩の **＋NEW** を押下します。
# MAGIC </br><img src="../images/genie.12.png" width="800"/>
# MAGIC
# MAGIC 以下の通りに `Genie` を設定したのち `Save` を押下します。
# MAGIC - **Title**：参加者全体で一意となるようあなたに固有の識別子を含めたタイトルを入力します。
# MAGIC - **Description**：以下に示す文言をコピーペーストします。
# MAGIC   ```
# MAGIC   基本的なふるまい：
# MAGIC   - 日本語で回答してください
# MAGIC   
# MAGIC   データセットについて:
# MAGIC   - Ringo Computer Company という法人向け PC、タブレット、スマートフォンを販売している会社の Sales Force Automation に関するデータセット
# MAGIC   ```
# MAGIC - **Default warehouse**：任意ですがここでは`Serverless Starter Warehouse`を指定しましょう。
# MAGIC </br><img src="../images/genie.2.png" width="800"/>
# MAGIC - **Tables**：上で作成したテーブル（全10テーブル）を指定します。対象の全テーブル名は以下に示すコマンドで確認可能です。
# MAGIC </br><img src="../images/genie.1.png" width="800"/>

# COMMAND ----------

# 対象テーブルの確認

table_list_df = spark.sql(f"SHOW TABLES IN {your_catalog}.{your_schema} LIKE '{table_name_prefix}*'")

with_cols_conf = {
    "Catalog": lit(your_catalog),
    "Scheam": lit(your_schema), 
    "Table": col("tableName"),
}
table_list_df = table_list_df.withColumns(with_cols_conf)
table_list_df = table_list_df.select(*with_cols_conf.keys())
table_list_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 動作確認
# MAGIC
# MAGIC チャットウィンドウに`データセットに含まれるテーブルについて説明して`と質問してみましょう。
# MAGIC </br><img src="../images/genie.4.png" width="800"/>
# MAGIC
# MAGIC 上のような回答が返ってきたはずです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## General Instructions による 回答のチューニング

# COMMAND ----------

# MAGIC %md
# MAGIC 次に Instructions により回答を少し改善してみましょう。
# MAGIC 左タブにある `指示` をクリックします。
# MAGIC </br><img src="../images/genie.13.png" width="800"/>
# MAGIC
# MAGIC 以下に示す文言を`General Instructions`にコピーペーストしたのち `Save` を押下します。
# MAGIC ```
# MAGIC 基本的な動作：
# MAGIC - 日本語で回答して
# MAGIC
# MAGIC データセットについて:
# MAGIC - Ringo Computer Company という法人向け PC、タブレット、スマートフォンを販売している会社の Sales Force Automation に関するデータセット
# MAGIC - lead -> opportunity -> order という順に営業活動が進みます
# MAGIC
# MAGIC テーブル名の概要:
# MAGIC
# MAGIC | テーブル        | 日本語テーブル名 | 概要                                                         |
# MAGIC | --------------- | ---------------- | ------------------------------------------------------------ |
# MAGIC | lead            | リード           | 潜在顧客の情報を管理するためのオブジェクト。                 |
# MAGIC | opportunity     | 商談             | 商談や販売機会の情報を管理するためのオブジェクト。           |
# MAGIC | order           | 注文             | 顧客からの注文情報を管理するためのオブジェクト。             |
# MAGIC | case            | ケース           | 顧客からの問い合わせやサポートリクエストの情報を管理するためのオブジェクト。 |
# MAGIC | account         | 取引先           | 取引先情報を管理するためのオブジェクト。顧客やパートナー企業などの情報を保持。 |
# MAGIC | contact         | 取引先責任者     | 取引先に関連する担当者情報を管理するためのオブジェクト。     |
# MAGIC | campaign        | キャンペーン     | マーケティングキャンペーンの情報を管理するためのオブジェクト。 |
# MAGIC | product         | 製品             | 販売する製品やサービスの情報を管理するためのオブジェクト。   |
# MAGIC | pricebook_entry | 価格表エントリ   | 製品の価格情報を管理するためのオブジェクト。                 |
# MAGIC | user            | ユーザー         | ユーザー情報（営業担当者）を管理するためのオブジェクト。     |
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 動作確認
# MAGIC 左上の`＋ 新しいチャット`を選択しあらためて`データセットに含まれるテーブルについて説明して`と質問してみましょう。
# MAGIC </br><img src="../images/genie.15.png" width="800"/>
# MAGIC
# MAGIC チャット履歴から前回との出力結果を比較して回答の改善を確認してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example SQL Queries による 回答のチューニング

# COMMAND ----------

# MAGIC %md
# MAGIC 次に `Show me the sales amount by order date.`という質問をしてみましょう。
# MAGIC </br><img src="../images/genie.7.png" width="800"/>
# MAGIC
# MAGIC 適切な回答が返ってきません。
# MAGIC
# MAGIC それでは先ほどの General Instructions とは異なる手段の改善方法を試してみましょう。
# MAGIC
# MAGIC 左タブにある `指示` から `SQLクエリーの例` までスクロールし`＋サンプルクエリ―を追加`を押下し以下に従いそれぞれ入力します。
# MAGIC
# MAGIC - **このクエリはどのような質問に答えていますか？**
# MAGIC   ```
# MAGIC   Show me the sales amount by order date.
# MAGIC   ```
# MAGIC
# MAGIC - **クエリ**  
# MAGIC 以下に示すコマンド結果をコピーペーストしてください。

# COMMAND ----------

# Example SQL Queries
table_name_prefix = "06_"
sql = f"""
SELECT
  CAST(ord.ActivatedDate AS DATE) AS order_date -- 注文日
  ,SUM(opp.TotalOpportunityQuantity * pbe.UnitPrice) AS total_ammount -- 受注金額
FROM
  {your_catalog}.{your_schema}.{table_name_prefix}order ord
INNER JOIN {your_catalog}.{your_schema}.{table_name_prefix}opportunity opp
ON 
  ord.OpportunityId__c = opp.Id
INNER JOIN {your_catalog}.{your_schema}.{table_name_prefix}product2 prd
ON 
  opp.Product2Id__c = prd.Id
INNER JOIN {your_catalog}.{your_schema}.{table_name_prefix}pricebook_entry pbe
ON 
  prd.Id = pbe.Product2Id
WHERE
  StatusCode = 'Completed'
GROUP BY ALL
""".strip()
print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC 入力が完了したら右下の`保存`を押下します。
# MAGIC </br><img src="../images/genie.17.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 動作確認
# MAGIC 左上の`＋ 新しいチャット`を選択しあらためて`Show me the sales amount by order date.`と質問してみましょう。
# MAGIC </br><img src="../images/genie.8.png" width="600"/>
# MAGIC
# MAGIC 上のような期待する結果が返ってくることを確認してください。
# MAGIC
# MAGIC もし期待する回答が得られない場合は、`Show me the sales amount by order date based on Example SQL Queries.`という質問で試してみてください。
# MAGIC
# MAGIC この結果は SQL による実行結果です。実行された SQL は `生成されたコードを表示` によって確認できます。
# MAGIC </br><img src="../images/genie.16.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL 関数 による 回答のチューニング
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 質問の回答生成に活用する テーブル値 SQL 関数を登録することもできます。
# MAGIC
# MAGIC まずは関数を定義しましょう。

# COMMAND ----------

function_name = "open_opps_in_states"
sql = f"""
CREATE
OR REPLACE FUNCTION {your_catalog}.{your_schema}.{function_name} (
  states ARRAY < STRING >
  COMMENT 'List of states.  Example: ["東京都", "大阪府", ]' DEFAULT NULL
) RETURNS TABLE
COMMENT 'Addresses questions about the pipeline in the specified states by returning
 a list of all the open opportunities. If no state is specified, returns all open opportunities.
 Example questions: "What is the pipeline for 東京駅 and 大阪府?", "Open opportunities in
 APAC"' RETURN
SELECT
  o.id AS `OppId`,
  a.BillingState AS `State`,
  o.name AS `Opportunity Name`,
  o.forecastcategory AS `Forecast Category`,
  o.stagename,
  o.closedate AS `Close Date`,
  o.amount AS `Opp Amount`
FROM
  {your_catalog}.{your_schema}.opportunity o
  JOIN {your_catalog}.{your_schema}.account a ON o.accountid = a.id
WHERE
  o.forecastcategory = 'Pipeline'
  AND o.stagename NOT LIKE '%closed%'
  AND (
    isnull({function_name}.states)
    OR array_contains({function_name}.states, BillingState)
  );
"""
spark.sql(sql)

print("-- Catalog")
print(your_catalog)
print()

print("-- Schema")
print(your_schema)
print()

print("-- Function")
print(function_name)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC 次に左タブにある `指示` から `SQL関数` までスクロールし`＋SQL関数を追加`を押下します。
# MAGIC
# MAGIC 上で作成したSQL関数を指定し`Save`を押下します。
# MAGIC </br><img src="../images/genie.18.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 動作確認
# MAGIC 左上の`＋ 新しいチャット`を選択し`What is the pipeline for 東京都 and 大阪府?`と質問してみましょう。
# MAGIC </br><img src="../images/genie.19.png" width="600"/>  
# MAGIC Genie が関数を使用して質問に答えるとその回答は「信頼できるアセット」を使用しているものとしてマークされます。
# MAGIC またこの結果は質問に対応するために自動生成された SQL による実行結果です。先ほどと同様に生成された SQL は `生成されたコードを表示` によって確認することができます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 参考リンク
# MAGIC
# MAGIC - [AI/BI Genie スペースとは](https://learn.microsoft.com/ja-jp/azure/databricks/genie/)
# MAGIC - [Genie 利用のベストプラクティス](https://learn.microsoft.com/ja-jp/azure/databricks/genie/best-practices)
