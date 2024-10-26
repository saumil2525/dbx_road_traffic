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
# MAGIC ## Reading data with More Columns
# MAGIC

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
        StructField("Max_Salary_USD", IntegerType()),
    ]
)


# COMMAND ----------

df_moreCols = (
    spark.read.format("csv")
    .schema(schema1)
    .option("header", "true")
    .load(f"{source}/SchemaEvol/SchemaMoreCols.csv")
)

# COMMAND ----------

df_moreCols.printSchema()

# COMMAND ----------

df_moreCols.write.format("delta").mode("append").saveAsTable("`delta`.deltaspark")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Source with Less Columns

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

schema = StructType(
    [
        StructField("Education_Level", StringType()),
        StructField("Line_Number", IntegerType()),
        StructField("Employed", IntegerType()),
        StructField("Unemployed", IntegerType()),
        StructField("Industry", StringType()),
        StructField("Gender", StringType()),
    ]
)

# COMMAND ----------

df_lessCols = (
    spark.read.format("csv")
    .schema(schema)
    .option("header", "true")
    .load(f"{source}/SchemaEvol/SchemaLessCols.csv")
)

# COMMAND ----------

df_lessCols.write.format("delta").mode("append").saveAsTable("`delta`.deltaspark")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `delta`.deltaspark

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Source data with different data type

# COMMAND ----------

df_diff = (
    spark.read.format("csv").option("header", "true").load(f"{source}/files/*.csv")
)

# COMMAND ----------

df_diff.write.format("delta").mode("append").saveAsTable("`delta`.deltaspark")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Allow changes for extra cols

# COMMAND ----------

df_moreCols.write.format("delta").mode("append").option(
    "mergeSchema", "True"
).saveAsTable("`delta`.deltaspark")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `delta`.deltaspark

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Allow different schema to evolve
# MAGIC

# COMMAND ----------

df_diff.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "True"
).saveAsTable("`delta`.deltaspark")

# COMMAND ----------
