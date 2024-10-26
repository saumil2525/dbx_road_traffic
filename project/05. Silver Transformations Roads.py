# Databricks notebook source
# MAGIC %run "/Workspace/project/04. Common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")


# COMMAND ----------

def read_brz_roads(environment):
    print('Reading the Bronze Table Data : ',end='')
    df_bronze_roads = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_roads")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_roads Success!')
    return df_bronze_roads

# COMMAND ----------

env

# COMMAND ----------

df_roads = read_brz_roads(env)

# COMMAND ----------

def road_category(df):
    print('Creating Road Category Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_cat = df.withColumn("Road_Category_Name",
                  when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                  .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                   .when(col('Road_Category') == 'PA','Class A Principal road')
                    .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                    .when(col('Road_Category') == 'M','Class B road')

                    .otherwise('NA')
                  )
    print('Success!! ')
    print('***********************')
    return df_road_cat

# COMMAND ----------

def road_type(df):
    print('Creating Road Type Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  )
    print('Success!! ')
    print('***********************')
    return df_road_type


# COMMAND ----------

def write_roads_silvertable(streaming_df, environment):
    print('Writing the silver_roads Data : ',end='') 

    write_stream_silver_roads = (streaming_df.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_roads`"))
    write_stream_silver_roads.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_roads` Success!')


# COMMAND ----------

df_dups = remove_dups(df_roads)
# remove nulls
Allcolumns =df_dups.schema.names
df_nulls = handle_nulls(df_dups, Allcolumns)

# create Road_Category_Name column
df_cat = road_category(df_nulls)

# creat Road_Type column
df_type = road_type(df_cat)

# write table
write_roads_silvertable(df_type, 'dev')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `dev_catalog`.`silver`.`silver_roads` limit 10

# COMMAND ----------

