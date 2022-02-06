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
# MAGIC ##### Step 1 - Ingest pitstops json file to spark dataframe(Multi line json file)

# COMMAND ----------

# Define the schema for the multi line json file.
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),\
                                          StructField("driverId", IntegerType(), False),\
                                          StructField("stop", IntegerType(), False),\
                                          StructField("lap", IntegerType(), False),\
                                          StructField("time", StringType(), False),\
                                          StructField("duration", StringType(), True),\
                                          StructField("milliseconds", IntegerType(), True)
                              ])

pit_stops_input_df = spark.read\
.schema(pit_stops_schema)\
.json(f'{input_folder_path}/pit_stops.json',multiLine=True)

display(pit_stops_input_df)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

pit_stops_selected_df = pit_stops_input_df.withColumnRenamed("raceId","race_id")\
                                      .withColumnRenamed("driverId","driver_Id")\
                                      .withColumn("ingestion_date",current_timestamp())
display(pit_stops_selected_df)

# COMMAND ----------

pit_stops_selected_df.write.mode("overwrite").parquet(f'{processed_folder_path}/pit_stops')
#display(spark.read.parquet(f'{processed_folder_path}/pit_stops"))



