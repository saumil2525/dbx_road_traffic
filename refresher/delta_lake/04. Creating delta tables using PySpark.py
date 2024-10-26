# Databricks notebook source
# Storage account credential
STORAGE_ACCOUNT = "XYZ"
APPLICATION_CLIENT_ID = "XYZ"
CLIENT_SECRET = "XYZ"
TENANT_ID = "XYZ"

# ContaIner
CONTAINER = "test"
FILE_PATH = "sample/*.csv"

# COMMAND ----------

# Set up the necessary configurations for Azure AD authentication
spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    APPLICATION_CLIENT_ID,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    CLIENT_SECRET,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
)

# COMMAND ----------

source = "abfss://test@deltadbstg.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the delta file into dataframe
# MAGIC

# COMMAND ----------

df = spark.read.format("parquet").load(f"{source}/ParquetFolder/")

# COMMAND ----------

display(df)

# COMMAND ----------

(df.write.format("delta").mode("overwrite").saveAsTable("`delta`.DeltaSpark"))

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/delta.db/deltaspark")

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/delta.db/deltaspark/_delta_log")

# COMMAND ----------

display(
    spark.read.format("text").load(
        "dbfs:/user/hive/warehouse/delta.db/deltaspark/_delta_log/00000000000000000000.json"
    )
)

# COMMAND ----------

display(
    spark.read.format("delta").load("dbfs:/user/hive/warehouse/delta.db/deltaspark")
)

# COMMAND ----------
