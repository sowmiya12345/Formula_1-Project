# Databricks notebook source
#dbutils.widgets.help()

#Commented after first time execution
#dbutils.widgets.text("data_source","")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run "/Users/vsowmiya28@gmail.com/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Users/vsowmiya28@gmail.com/Formula1/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step -1 Ingest results json data to Dataframe(Single line json)

# COMMAND ----------

# Define the schema for the single line json file.
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

results_schema = StructType(fields = [StructField("raceId", IntegerType(), False),\
                                          StructField("driverId", IntegerType(), False),\
                                          StructField("constructorId", IntegerType(), False),\
                                          StructField("number", IntegerType(), True),\
                                          StructField("grid", IntegerType(), False),\
                                          StructField("position", IntegerType(), True),\
                                          StructField("positionText", StringType(), False),\
                                          StructField("positionOrder", IntegerType(), False),\
                                          StructField("points", FloatType(), False),\
                                          StructField("laps", IntegerType(), False),\
                                          StructField("time", StringType(), True),\
                                          StructField("milliseconds", IntegerType(), True),\
                                          StructField("fastestLap", IntegerType(), True),\
                                          StructField("rank", IntegerType(), True),\
                                          StructField("fastestLapTime", StringType(), True),\
                                          StructField("fastestLapSpeed", StringType(), True),\
                                          StructField("statusId", IntegerType(), False)
                              ])

results_input_df = spark.read \
.schema(results_schema) \
.json(f'{input_folder_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Data Cleaning(Adding,Renaming,Removing columns)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

results_selected_df = results_input_df.drop(col("url"))\
                                      .withColumnRenamed("resultId","result_id")\
                                      .withColumnRenamed("raceId","race_id")\
                                      .withColumnRenamed("driverId","driver_Id")\
                                      .withColumnRenamed("constructorId","constructor_id")\
                                      .withColumnRenamed("positionText","position_text")\
                                      .withColumnRenamed("positionOrder","position_order")\
                                      .withColumnRenamed("fastestLap","fastest_lap")\
                                      .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                      .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                      .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write result to parquet file paritioned based on race_id

# COMMAND ----------

results_selected_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")
display(spark.read.parquet("/FileStore/tables/processed/results"))


