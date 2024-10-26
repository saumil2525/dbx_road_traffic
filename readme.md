## Overview

The Databricks Road Traffic Project analyzes road traffic data using Databricks. Additionally, This project demonstrates some of the features of the databricks however these are not comprehensive set of the features which databrics offers. The project involves setting up schemas, loading raw data into bronze tables, transforming the data into silver tables, and performing final transformations to create gold tables. The project also includes validation scripts to ensure data quality.

The key objectives of the project are:
- To ingest and store raw traffic and road data.
- To perform data transformations and aggregations.
- To create a structured and efficient data pipeline.
- To validate the data at each stage of the pipeline.

The project is structured into various scripts and directories, each serving a specific purpose in the data pipeline. The scripts are written in Python and SQL, leveraging Databricks' capabilities for data processing and analysis.

## Key Files and Directories

- **dbx_road_traffic/project/**: Contains various project scripts for setting up schemas, loading data, and performing transformations.
  - **[01 Project Setup.py](dbx_road_traffic/project/01%20Project%20Setup.py)**: Contains functions for creating schemas and tables.
  - **[02. Load to bronze.py](dbx_road_traffic/project/02.%20Load%20to%20bronze.py)**: Contains functions for ingesting and writing raw traffic and roads data to the bronze schema.
  - **[03. Silver Transformations Traffic.py](dbx_road_traffic/project/03.%20Silver%20Transformations%20Traffic.py)**: Contains functions for reading data from the bronze schema.
  - **[05. Silver Transformations Roads.py](dbx_road_traffic/project/05.%20Silver%20Transformations%20Roads.py)**: Contains functions for reading roads data from the bronze schema.
  - **[06. Gold Final Transformation.py](dbx_road_traffic/project/06.%20Gold%20Final%20Transformation.py)**: Contains functions for reading data from the silver schema.
  - **[07. Validation.sql](dbx_road_traffic/project/07.%20Validation.sql)**: Contains SQL queries for validating data in different schemas.
- **dbx_road_traffic/refresher/dbx_basics/**: Contains notebooks and scripts for basic Databricks operations.
- **dbx_road_traffic/data/**: Contains configuration files and other data-related resources.

## Important Scripts

### [01 Project Setup.py](dbx_road_traffic/project/01%20Project%20Setup.py)

- **`create_schema(environment, target_schema, path)`**: Creates a schema in the specified environment and path.
- **`creating_raw_traffic(environment)`**: Creates the `raw_traffic` table in the bronze schema of the specified environment.
- **`creating_raw_roads(environment)`**: Creates the `raw_roads` table in the bronze schema of the specified environment.

## Usage

To set up the project, run the `create_schema` function with the appropriate parameters for your environment. Then, use the `creating_raw_traffic` and `creating_raw_roads` functions to create the necessary tables.

```python
create_schema('dev', 'target_schema', '/path/to/schema')
creating_raw_traffic('dev')
creating_raw_roads('dev')