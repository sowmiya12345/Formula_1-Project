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
# MAGIC ##### Step - 1 Ingest data from multiple files(Folder) to spark dataframe

# COMMAND ----------

# Define the schema of csv files

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),\
                              StructField("driverId", IntegerType(), False),\
                              StructField("lap", IntegerType(), False),\
                              StructField("position", IntegerType(), True),\
                              StructField("time", StringType(), True),\
                              StructField("milliseconds", IntegerType(), True),
                              ])

lap_times_input_df = spark.read \
.schema(lap_times_schema)\
.csv(f'{input_folder_path}/lap_times/lap_times*.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Data Cleaning(Add,Remove,Rename columns)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
lap_times_selected_df = lap_times_input_df.withColumnRenamed("raceId","race_id")\
                                          .withColumnRenamed("driverId","driver_id")\
                                          .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Write data to datalake in parquet format

# COMMAND ----------

lap_times_selected_df.write.mode("overwrite").parquet(f'{processed_folder_path}/lap_times')
