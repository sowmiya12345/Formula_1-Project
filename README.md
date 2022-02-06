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


