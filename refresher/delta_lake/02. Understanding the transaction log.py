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

dbutils.fs.ls(f"{source}/delta/")

# COMMAND ----------

dbutils.fs.ls(f"{source}/delta/_delta_log")

# COMMAND ----------

display(
    spark.read.format("text").load(
        "abfss://test@deltadbstg.dfs.core.windows.net/delta/_delta_log/00000000000000000000.json"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the delta lake file

# COMMAND ----------

df = spark.read.format("delta").load(f"{source}/delta/")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df_delta = df.filter("Education_Level =='High School'")

# COMMAND ----------

df_delta.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overwriting the same file in delta folder

# COMMAND ----------

(df_delta.write.format("delta").mode("overwrite").save(f"{source}/delta/"))

# COMMAND ----------

display(
    spark.read.format("text").load(
        "abfss://test@deltadbstg.dfs.core.windows.net/delta/_delta_log/00000000000000000001.json"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the overwritten file

# COMMAND ----------

df_overwrite = spark.read.format("delta").load(f"{source}/delta/")

# COMMAND ----------

display(df_overwrite)

# COMMAND ----------
