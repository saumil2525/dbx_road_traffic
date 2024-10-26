# Databricks notebook source
# MAGIC %run "/Workspace/project/04. Common"

# COMMAND ----------

dbutils.widgets.text(name="env", defaultValue="", label=" Enter the environ ment in lower case")
# dbutils.widgets.text(name="env", defaultValue="", label=" Enter the environ ment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Write function

# COMMAND ----------

def ingest_data_raw_traffic():
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    from pyspark.sql.functions import current_timestamp

    schema = StructType([
        StructField("Record_ID",IntegerType()),
        StructField("Count_point_id",IntegerType()),
        StructField("Direction_of_travel",StringType()),
        StructField("Year",IntegerType()),
        StructField("Count_date",StringType()),
        StructField("hour",IntegerType()),
        StructField("Region_id",IntegerType()),
        StructField("Region_name",StringType()),
        StructField("Local_authority_name",StringType()),
        StructField("Road_name",StringType()),
        StructField("Road_Category_ID",IntegerType()),
        StructField("Start_junction_road_name",StringType()),
        StructField("End_junction_road_name",StringType()),
        StructField("Latitude",DoubleType()),
        StructField("Longitude",DoubleType()),
        StructField("Link_length_km",DoubleType()),
        StructField("Pedal_cycles",IntegerType()),
        StructField("Two_wheeled_motor_vehicles",IntegerType()),
        StructField("Cars_and_taxis",IntegerType()),
        StructField("Buses_and_coaches",IntegerType()),
        StructField("LGV_Type",IntegerType()),
        StructField("HGV_Type",IntegerType()),
        StructField("EV_Car",IntegerType()),
        StructField("EV_Bike",IntegerType())
        ])
    
    read_raw_traffic = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "csv")
                        .option("cloudFiles.schemaLocation", f"{checkpoint}/raw_trafficLoad/schemaInfer")
                        .option('header', 'true')
                        .schema(schema=schema)
                        .load(landing + '/raw_traffic/')
                        .withColumn("Extract_Time", current_timestamp())) 

    print("\nReading success !! ")
    # read_raw_traffic.printSchema()
    print("***************************")

    return read_raw_traffic


def write_stream_raw_traffic(environment, read_streaming_df):
    write_stream = (read_streaming_df.writeStream
                    .format('delta')
                    .option("checkpointLocation", checkpoint + "/raw_trafficLoad/Checkpoint")
                    .outputMode('append')
                    .queryName("rawTrafficWriteStream")
                    .trigger(availableNow=True)
                    .toTable(f'`{environment}_catalog`.`bronze`.`raw_traffic`'))
    write_stream.awaitTermination()
    print("\nWrite Successful !!")
    print("***************************")


def ingest_data_raw_roads():
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    from pyspark.sql.functions import current_timestamp

    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
    ])
    
    read_raw_roads = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "csv")
                        .option("cloudFiles.schemaLocation", f"{checkpoint}/raw_roadsLoad/schemaInfer")
                        .option('header', 'true')
                        .schema(schema=schema)
                        .load(landing + '/raw_roads/')
                        ) 

    print("\nReading success !! ")
    # read_raw_roads.printSchema()
    print("***************************")

    return read_raw_roads


def write_stream_raw_roads(environment, read_streaming_df):
    write_stream = (read_streaming_df.writeStream
                    .format('delta')
                    .option("checkpointLocation", checkpoint + "/raw_roadsLoad/Checkpoint")
                    .outputMode('append')
                    .queryName("rawRoadsWriteStream")
                    .trigger(availableNow=True)
                    .toTable(f'`{environment}_catalog`.`bronze`.`raw_roads`'))
    write_stream.awaitTermination()
    print("\nWrite Successful !! ")
    print("***************************")

def write_stream(environment, read_streaming_df, schema='bronze', table_name='raw_roads'):
    write_stream = (read_streaming_df.writeStream
                    .format('delta')
                    .option("checkpointLocation", checkpoint + f"/{table_name}Load/Checkpoint")
                    .outputMode('append')
                    .queryName(f"{table_name}WriteStream")
                    .trigger(availableNow=True)
                    .toTable(f'`{environment}_catalog`.`{schema}`.`{table_name}`'))
    write_stream.awaitTermination()
    print(f"\nWrite Successful !! : {table_name}")
    print("***************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calling functions

# COMMAND ----------

# reading raw traffic data
read_raw_traffic_df = ingest_data_raw_traffic()
# reading raw roads data
read_raw_roads_df = ingest_data_raw_roads()

# writing raw traffic data to bronze
write_stream(env, read_raw_traffic_df, schema='bronze', table_name='raw_traffic')
# writting raw roads to bronze
write_stream(env, read_raw_roads_df, schema='bronze', table_name='raw_roads')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `dev_catalog`.`bronze`.raw_traffic limit 5; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `dev_catalog`.`bronze`.raw_roads limit 5;

# COMMAND ----------

# %sql
# -- Drop all tables in the bronze schema
# DROP TABLE IF EXISTS `dev_catalog`.`bronze`.`raw_traffic`;

# -- Drop all tables in the bronze schema
# DROP TABLE IF EXISTS `dev_catalog`.`bronze`.`raw_traffic`;

# -- Drop the bronze schema
# DROP SCHEMA IF EXISTS `dev_catalog`.`bronze` CASCADE;

# -- Drop the gold schema
# DROP SCHEMA IF EXISTS `dev_catalog`.`gold` CASCADE;

# -- Drop the silver schema
# DROP SCHEMA IF EXISTS `dev_catalog`.`silver` CASCADE;

# -- Drop the catalog
# -- DROP CATALOG IF EXISTS `dev_catalog`  CASCADE;