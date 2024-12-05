# Databricks notebook source
# MAGIC %md
# MAGIC ## Power BI によるデータ可視化
# MAGIC
# MAGIC 1. Power BI Desktop を起動し、**データを取得**から  **Azure Databricks** を選択し**接続**を押下します。
# MAGIC </br><img src="images/05.3.19.png" width="600" />  
# MAGIC Azure Databricks への接続情報を入力し **OK** を押下します。
# MAGIC </br><img src="images/05.3.20.png" width="600" />  
# MAGIC Azure Databricks への接続情報は **SQL ウェアハウス**の **接続の詳細**で確認できます。  
# MAGIC </br><img src="images/05.3.21.png" width="600" />  
# MAGIC もしくは右上上部のドロップダウンメニュから `次で開く：Power BI デスクトップ`を選択し**接続ファイルをダウンロード**を押下しダウンロードした pbix ファイル を開くことでも同様です。
# MAGIC </br><img src="images/05.3.1.png" width="600" />  
# MAGIC </br><img src="images/05.3.2.png" width="600" />  
# MAGIC 1. Azure Entra ID 認証を行ったのち Databricks へ接続します。
# MAGIC </br><img src="images/05.3.3.png" width="300" />
# MAGIC 1. スキーマの一覧から `05_gold_nyctaxi` を選択します。
# MAGIC </br><img src="images/05.3.4.png" width="600" />
# MAGIC 1. 下記を例に Power BI デスクトップ を利用しデータセットを可視化します。
# MAGIC </br><img src="images/05.3.18.png" width="600" />
# MAGIC </br><img src="images/05.3.5.png" width="600" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks ダッシュボード によるデータ可視化
# MAGIC
# MAGIC 1. 右上上部のドロップダウンメニュから`ダッシュボードで開く`を押下します。
# MAGIC </br><img src="images/05.3.6.png" width="600" />
# MAGIC 1. ウィジェット内の AI アシスタントの候補から例えば `Total Trip Distance by Pickup Zone` を選択します。
# MAGIC </br><img src="images/05.3.7.png" width="600" />
# MAGIC 1. AI アシスタントにより自動的に可視化が行われます。この可視化をベースに右のメニュでカスタマイズも可能です。またこの可視化のソースとなったデータセットが確認できます。
# MAGIC </br><img src="images/05.3.8.png" width="600" />
# MAGIC 1. 左上上部のタブメニュから`データ`を選択するとデータセットが一覧表示されます。データセットはクエリで構成されていることが確認できます。ここでは新たにデータセットを追加することも可能です。
# MAGIC </br><img src="images/05.3.9.png" width="600" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## オプション：Power BI Service への発行
# MAGIC
# MAGIC 1. 右上上部のドロップダウンメニュから`公開先：Power BI ワークスペース`を押下します。
# MAGIC </br><img src="images/05.3.10.png" width="600" />
# MAGIC 1. 公開先のテナントに接続します。
# MAGIC </br><img src="images/05.3.11.png" width="600" />
# MAGIC 1. この操作には管理者アカウントによる承認が必要です。
# MAGIC </br><img src="images/05.3.12.png" width="200" />
# MAGIC 1. 公開先のワークスペースを選択し `Power BI に接続` を押下します。
# MAGIC </br><img src="images/05.3.13.png" width="600" />
# MAGIC 1. 正常に公開されたのち `Power BI を開く` を押下します。
# MAGIC </br><img src="images/05.3.14.png" width="600" />
# MAGIC 1. Power BI ワークスペース上にデータセットが作成されていることが確認できます。
# MAGIC </br><img src="images/05.3.15.png" width="600" />
# MAGIC 1. ここではレポート作成の例としてリボンメニュから `レポートの自動作成` を選択します。
# MAGIC </br><img src="images/05.3.16.png" width="600" />
# MAGIC 1. 連携されたデータをもとにレポートが自動作成されます。
# MAGIC </br><img src="images/05.3.17.png" width="600" />
# MAGIC
# MAGIC #### 参考
# MAGIC  - [Announcing General Availability: Publish to Microsoft Power BI Service from Unity Catalog](https://www.databricks.com/blog/announcing-general-availability-publish-microsoft-power-bi-service-unity-catalog)
# MAGIC   - [Publish to Power BI Online from Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi#publish)
