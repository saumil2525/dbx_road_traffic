# Databricks notebook source
# MAGIC %md
# MAGIC Define all common variable

# COMMAND ----------

# get variables
checkpoint = spark.sql("describe external location `checkpoints-uc-project-external-loc`").select("url").collect()[0][0]
landing = spark.sql("describe external location `landing-uc-project-external-loc`").select("url").collect()[0][0]
bronze = spark.sql("describe external location `bronze-uc-project-external-loc`").select("url").collect()[0][0]
silver = spark.sql("describe external location `silver-uc-project-external-loc`").select("url").collect()[0][0]
gold = spark.sql("describe external location `gold-uc-project-external-loc`").select("url").collect()[0][0]


# COMMAND ----------

# Remove dups
def remove_dups(df):
    print('Removing Duplicate values: ',end='')
    df_dup = df.dropDuplicates()
    print('Success!')
    print('***********************')
    return df_dup

# COMMAND ----------

# Remove nulls
def handle_nulls(df,Columns):
    print('Replacing NULLs of Strings DataType with "Unknown": ', end='')
    df_string = df.fillna('Unknown',subset=Columns)
    print('Success!')
    print('Replacing NULLs of Numeric DataType with "0":  ', end='')
    df_numeric = df_string.fillna(0,subset=Columns)
    print('Success!')
    print('***********************')
    return df_numeric

# COMMAND ----------

print("Notebook run completed.")