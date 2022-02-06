# FORMULA_1 RACE PROJECT
Formula_1 Pyspark Project using Databricks.
End-to-End Big Data Pipeline is built to fetch Formula_1 race data 
from Ergast API is ingested, processed, analyzed using Databricks(DBFS).
## TECHNOLOGY AND TOOLS USED:
Technology - Big Data, Data Processing - Pyspark, Data Storage - Databricks File System(DBFS)
## BIG DATA PIPELINE ARCHITECTURE:
![image](https://user-images.githubusercontent.com/56109382/152692820-94e77036-a999-4de5-9ad5-8c87cd3276c5.png)
## FORMULA 1 RACE INPUT DATA FILES:
<img width="318" alt="Formula_1 Data Files (2)" src="https://user-images.githubusercontent.com/56109382/152693397-44419aed-de5d-420f-a565-20a9bdeea248.PNG">

## Data Ingestion Requirements:
1. Ingest formula 1 data (all 8 files) into DBFS.
2. Ingested data must have the schema specified.
3. Ingested data must have the audit columns.
4. Ingested data must be in columnar format(parquet).
5. Must be able to analyze the ingested data via SQL.

## Data Transformation Requirements:
1. Transform the data to obtain the race_results.
2. Transform the data to obtain the driver_standings.
3. Transform the data to obtain the constructor_standings.

## Input files Directory Structure in DBFS:
<img width="806" alt="formula_1 input files Directory structure in DBFS" src="https://user-images.githubusercontent.com/56109382/152695159-62b7af36-9ee3-4bbf-af92-62888836f49a.PNG">

## Ingested data Directory Structure in DBFS:
<img width="766" alt="formula_1 ingested data folder structure" src="https://user-images.githubusercontent.com/56109382/152695168-30ed723e-c870-450e-9b54-210281c02aa5.PNG">

## Transformed data(Final Output) Directory Structure in DBFS:
<img width="811" alt="formula_1 output folder structure in DBFS" src="https://user-images.githubusercontent.com/56109382/152695179-4ebb3091-a482-4c78-9c4a-7b05cc0a2da7.PNG">

