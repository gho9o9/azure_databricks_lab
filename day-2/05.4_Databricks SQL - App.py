# Databricks notebook source
# MAGIC %md
# MAGIC ## 各種アプリケーションからの接続
# MAGIC
# MAGIC 1. サイドバーの **SQL ウェアハウス** をクリックし、いずれかの SQL ウェアハウスを選択します。
# MAGIC </br><img src="../images/05.4.1.png" width="600" />
# MAGIC 1. ここでは各種アプリケーションから接続する際の接続文字列が確認できます。
# MAGIC </br><img src="../images/05.4.2.png" width="600" />
# MAGIC 1. またアプリケーションのアイコンを選択すると該当アプリケーションコードによるサンプルコードを確認できます。
# MAGIC </br><img src="../images/05.4.3.png" width="600" />
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 例：HTTP クライアント（Postman）からのクエリ
# MAGIC
# MAGIC 1. SQL ウェアハウスの**接続の詳細**で確認できる`ホスト名`を指定します。また、ヘッダの `Authorization` に `Databricks へのアクセストークン` を指定します。
# MAGIC </br><img src="../images/05.4.4.png" width="600" />
# MAGIC 1. SQL ウェアハウスの**接続の詳細**で確認できる`HTTPパス`をボディの`warehouse_id`に指定します。また、`クエリ`をボディの`statement`に指定します。
# MAGIC </br><img src="../images/05.4.5.png" width="600" />
# MAGIC 1. **送信**を押下し REST API を実行します。
# MAGIC </br><img src="../images/05.4.6.png" width="600" />
# MAGIC
# MAGIC #### 参考
# MAGIC - [Azure Databricks REST API reference](https://docs.databricks.com/api/azure/workspace/introduction)
# MAGIC - [Execute a SQL statement](https://docs.databricks.com/api/azure/workspace/statementexecution/executestatement)
