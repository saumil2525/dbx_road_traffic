# Databricks notebook source
# MAGIC %run "/Workspace/project/04. Common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")


# COMMAND ----------

def read_silver_traffic_table(environment):
    print('Reading the Silver Traffic Table Data : ',end='')
    df_silvertraffic = (spark.readStream
                    .table(f"`{environment}_catalog`.`silver`.silver_traffic")
                    )
    print(f'Reading {environment}_catalog.silver.silver_traffic Success!')
    print("**********************************")
    return df_silvertraffic

# COMMAND ----------

def read_silver_roads_table(environment):
    print('Reading the Silver Table Silver_roads Data : ',end='')
    df_silverroads = (spark.readStream
                    .table(f"`{environment}_catalog`.`silver`.silver_roads")
                    )
    print(f'Reading {environment}_catalog.silver.silver_roads Success!')
    print("**********************************")
    return df_silverroads

# COMMAND ----------

def create_vehicle_intensity(df):
    from pyspark.sql.functions import col
    print('Creating Vehicle Intensity column : ',end='')
    df_veh = df.withColumn('Vehicle_Intensity',
                col('Motor_Vehicles_Count') / col('Link_length_km')
                )
    print("Success!!!")
    print('***************')
    return df_veh

# COMMAND ----------

def create_load_time(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Load Time column : ',end='')
    df_timestamp = df.withColumn('Load_Time',
                      current_timestamp()
                      )
    print('Success!!')
    print('**************')
    return df_timestamp

# COMMAND ----------

def write_traffic_gold_table(streaming_df,environment):
    print('Writing the gold_traffic Data : ',end='') 

    write_gold_traffic = (streaming_df.writeStream
                .format('delta')
                .option('checkpointLocation', checkpoint + "GoldTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`gold_traffic`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`gold_traffic` Success!')


# COMMAND ----------

def write_roads_gold_table(StreamingDF,environment):
    print('Writing the gold_roads Data : ',end='') 

    write_gold_roads = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation', checkpoint + "GoldRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`gold_roads`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`gold_roads` Success!')


# COMMAND ----------

## Reading from Silver tables
df_silver_traffic = read_silver_traffic_table(env)
df_silver_roads = read_silver_roads_table(env)
    
## Tranformations     
df_vehicle = create_vehicle_intensity(df_silver_traffic)
df_FinalTraffic = create_load_time(df_vehicle)
df_FinalRoads = create_load_time(df_silver_roads)


## Writing to gold tables    
write_traffic_gold_table(df_FinalTraffic, env)
write_roads_gold_table(df_FinalRoads, env)

# COMMAND ----------

