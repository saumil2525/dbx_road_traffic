# Databricks notebook source
# MAGIC %run "/Workspace/project/04. Common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")


# COMMAND ----------



# COMMAND ----------

def read_brz_trafic(environment):
    print('Reading the Bronze Table Data : ',end='')
    df_bronzeTraffic = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_traffic")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_traffic Success!')
    return df_bronzeTraffic


# COMMAND ----------

def remove_dups(df):
    print('Removing Duplicate values: ', end='')
    df_dup = df.dropDuplicates()
    print('Success!! ')
    return df_dup

# COMMAND ----------

def handle_nulls(df,columns):
    print('Replacing NULL values on String Columns with "Unknown" ' , end='')
    df_string = df.fillna('Unknown',subset= columns)
    print('Successs!! ')

    print('Replacing NULL values on Numeric Columns with "0" ' , end='')
    df_clean = df_string.fillna(0,subset = columns)
    print('Success!! ')

    return df_clean

# COMMAND ----------

def ev_count(df):
    print('Creating Electric Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_ev = df.withColumn('Electric_Vehicles_Count',
                            col('EV_Car') + col('EV_Bike')
                            )
    
    print('Success!! ')
    return df_ev

# COMMAND ----------

def motor_count(df):
    print('Creating All Motor Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_motor = df.withColumn('Motor_Vehicles_Count',
                            col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type')
                            )
    
    print('Success!! ')
    return df_motor

# COMMAND ----------

def create_transformedtime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Transformed Time column : ',end='')
    df_timestamp = df.withColumn('Transformed_Time',
                      current_timestamp()
                      )
    print('Success!!')
    return df_timestamp

# COMMAND ----------

def write_traffic_silvertable(StreamingDF,environment):
    print('Writing the silver_traffic Data : ',end='') 

    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation', checkpoint + "/SilverTrafficLoad/Checkpoint/")
                .outputMode('append')
                .queryName("SilverTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_traffic`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_traffic` Success!')


# COMMAND ----------

#read data
df_trafficdata = read_brz_trafic(env)

# remove dups
df_dups = remove_dups(df_trafficdata)

# To raplce any NULL values
Allcolumns =df_dups.schema.names
df_nulls = handle_nulls(df_dups, Allcolumns)

## To get the total EV_Count
df_ev = ev_count(df_nulls)

## To get the Total Motor vehicle count
df_motor = motor_count(df_ev)

## Calling Transformed time function
df_final = create_transformedtime(df_motor)

## Writing to silver_traffic
write_traffic_silvertable(df_final, env)