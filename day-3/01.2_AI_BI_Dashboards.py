# Databricks notebook source
# MAGIC %md
# MAGIC # AI/BI Dashboards によるデータ可視化

# COMMAND ----------

# MAGIC %md
# MAGIC <a href="$./01.1_AI_BI_Genie"> AI/BI Genie ラボ</a>で作成したテーブルを利用して自由にダッシュボードを作成してみてください。
# MAGIC
# MAGIC ダッシュボードの作成例は以下の動画を参考にしてください。  
# MAGIC - https://www.youtube.com/watch?v=zrYAOUcpyDQ
# MAGIC
# MAGIC 以下にダッシュボードで利用できるデータセットの例をいくつか紹介します。
# MAGIC - ケース
# MAGIC - 問い合わせ
# MAGIC - クレーム
# MAGIC - 未クローズ
# MAGIC - 未クローズかつ優先度高い
# MAGIC - ケースのレビュー
# MAGIC

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

print("-- ケース")
case = f"""select * from {your_catalog}.{your_schema}.case
"""
print(case)


print("-- 問い合わせ")
query = f"""select * from {your_catalog}.{your_schema}.case
where type = "問い合わせ"
"""
print(query)


print("-- クレーム")
claim = f"""SELECT * FROM {your_catalog}.{your_schema}.case
WHERE Type = 'クレーム';
"""
print(claim)


print("-- 未クローズ")
not_closed = f"""SELECT * FROM {your_catalog}.{your_schema}.case
WHERE IsClosed = false;
"""
print(not_closed)


print("-- 未クローズかつ優先度高い")
not_closed_high_priority = f"""SELECT * FROM {your_catalog}.{your_schema}.case
WHERE IsClosed = false and Priority = "高";
"""
print(not_closed_high_priority)


print("-- ケースのレビューテーブル作成")
review_ctas = f"""CREATE TABLE {your_catalog}.{your_schema}.case_classified AS 
SELECT *, ai_classify(
    Description,
    ARRAY(
      "ソフトウェアのバグ",
      "ハードウェアの動作不良",
      "ハードウェアの破損",
      "ネットワークの動作不良",
      "その他"
    )
  ) AS predict
FROM {your_catalog}.{your_schema}.`case`
LIMIT 100;
"""
print(review_ctas)


print("-- ケースのレビュー")
review = f"""SELECT * FROM {your_catalog}.{your_schema}.case_classified"""
print(review)
