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
# MAGIC ### Reading data from CSV file

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    FloatType,
    DoubleType,
)

schema1 = StructType(
    [
        StructField("Education_Level", StringType()),
        StructField("Line_Number", IntegerType()),
        StructField("Employed", IntegerType()),
        StructField("Unemployed", IntegerType()),
        StructField("Industry", StringType()),
        StructField("Gender", StringType()),
        StructField("Date_Inserted", StringType()),
        StructField("dense_rank", IntegerType()),
    ]
)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema1)
    .load(f"{source}/files/*.csv")
)

# COMMAND ----------

df.write.format("parquet").save(f"{source}/OnlyParquet")

# COMMAND ----------

dfnew = df.withColumnRenamed(existing="Line_Number", new="LNo")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Overwriting Parquet

# COMMAND ----------

dfnew.write.format("parquet").mode("overwrite").save(f"{source}/OnlyParquet")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading overwritten parquet

# COMMAND ----------

df_ov = spark.read.format("parquet").load(f"{source}/OnlyParquet")

# COMMAND ----------

display(df_ov)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA parquet.`abfss://test@deltadbstg.dfs.core.windows.net/OnlyParquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`abfss://test@deltadbstg.dfs.core.windows.net/OnlyParquet`

# COMMAND ----------
