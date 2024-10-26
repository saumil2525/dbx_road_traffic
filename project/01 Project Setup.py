# Databricks notebook source
# MAGIC %md
# MAGIC ## Get param value
# MAGIC

# COMMAND ----------

dbutils.widgets.text(
    name="env", defaultValue="", label=" Enter the environment in lower case"
)
# dbutils.widgets.text(name="env", defaultValue="", label=" Enter the environ ment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating All Functions

# COMMAND ----------


def create_schema(environment, target_schema, path):
    print(f"\nUsing {environment}_Catalog")
    spark.sql(f"""USE CATALOG '{environment}_catalog'""")
    print(f"Creating {target_schema} schema in {environment}_Catalog")
    spark.sql(
        f"""CREATE SCHEMA IF NOT EXISTS `{target_schema}` MANAGED LOCATION '{path}/{target_schema}'"""
    )
    print("**************************************")


def creating_raw_traffic(environment):
    print("\nCreating raw_traffic in bronze schema of {environment}_catalog ")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_traffic`
              (
                    Record_ID INT,
                    Count_point_id INT,
                    Direction_of_travel VARCHAR(255),
                    Year INT,
                    Count_date VARCHAR(255),
                    hour INT,
                    Region_id INT,
                    Region_name VARCHAR(255),
                    Local_authority_name VARCHAR(255),
                    Road_name VARCHAR(255),
                    Road_Category_ID INT,
                    Start_junction_road_name VARCHAR(255),
                    End_junction_road_name VARCHAR(255),
                    Latitude DOUBLE,
                    Longitude DOUBLE,
                    Link_length_km DOUBLE,
                    Pedal_cycles INT,
                    Two_wheeled_motor_vehicles INT,
                    Cars_and_taxis INT,
                    Buses_and_coaches INT,
                    LGV_Type INT,
                    HGV_Type INT,
                    EV_Car INT,
                    EV_Bike INT,
                    Extract_Time TIMESTAMP
                    )"""
    )
    print("**************************************")


def creating_raw_roads(environment):
    print(f"\nCreating raw_roads in bronze schema of {environment}_catalog ")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_roads`
                (
                    Road_ID INT,
                    Road_Category_Id INT,
                    Road_Category VARCHAR(255),
                    Region_ID INT,
                    Region_Name VARCHAR(255),
                    Total_Link_Length_Km DOUBLE,
                    Total_Link_Length_Miles DOUBLE,
                    All_Motor_Vehicles DOUBLE
                    )"""
    )
    print("**************************************")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Calling functions

# COMMAND ----------

# Get paths
bronze_path = (
    spark.sql(""" DESCRIBE  EXTERNAL LOCATION `bronze-uc-project-external-loc`""")
    .select("url")
    .collect()[0][0]
)
silver_path = (
    spark.sql(""" DESCRIBE  EXTERNAL LOCATION `silver-uc-project-external-loc`""")
    .select("url")
    .collect()[0][0]
)
gold_path = (
    spark.sql(""" DESCRIBE  EXTERNAL LOCATION `gold-uc-project-external-loc`""")
    .select("url")
    .collect()[0][0]
)

# Create Schema

create_schema(env, "bronze", bronze_path)
create_schema(env, "silver", silver_path)
create_schema(env, "gold", gold_path)

# Create Tables
creating_raw_traffic(environment=env)
creating_raw_roads(environment=env)
