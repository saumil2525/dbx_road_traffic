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

df.createOrReplaceTempView("df_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Creating a delta table
# MAGIC
# MAGIC CREATE TABLE delta.VacTable
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM df_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.VacTable

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/delta.db/vactable")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Performing multiple inserts
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('Bachelor', 1, 4500, 500, 'Networking', 'Male', '2023-07-12', 1);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('Master', 2, 6500, 500, 'Networking', 'Female', '2023-07-12', 2);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('High School', 3, 3500, 500, 'Networking', 'Male', '2023-07-12', 3);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('PhD', 4, 5500, 500, 'Networking', 'Female', '2023-07-12', 4);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Performing updates
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform updates
# MAGIC
# MAGIC UPDATE delta.VacTable
# MAGIC SET Education_Level = 'Phd'
# MAGIC WHERE Industry = 'Networking';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Perfroming deletes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform delete
# MAGIC
# MAGIC DELETE FROM delta.VacTable
# MAGIC WHERE Education_Level = 'Phd';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY delta.vactable

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/delta.db/vactable")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM `delta`.Vactable DRY RUN
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM `delta`.Vactable RETAIN 5 HOURS

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "False")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM `delta`.Vactable RETAIN 5 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM `delta`.Vactable RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY `delta`.Vactable

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/delta.db/vactable")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY `delta`.Vactable

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/delta.db/vactable")

# COMMAND ----------
