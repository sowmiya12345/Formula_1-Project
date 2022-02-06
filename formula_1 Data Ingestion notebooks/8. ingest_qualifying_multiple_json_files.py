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
# MAGIC ##### Step 1 - Ingest Qualifying multiple json files(from folder) to spark dataframe(Multi line json file)

# COMMAND ----------

# Define the schema for the multi line json mutliple json file.
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),\
                                          StructField("raceId", IntegerType(), False),\
                                          StructField("driverId", IntegerType(), False),\
                                          StructField("constructorId", IntegerType(), False),\
                                          StructField("number", IntegerType(), False),\
                                          StructField("position", IntegerType(), True),\
                                          StructField("q1", StringType(), True),\
                                          StructField("q2", StringType(), True),\
                                          StructField("q3", StringType(), True)
                             ])

qualifying_input_df = spark.read\
.schema(qualifying_schema)\
.json(f'{input_folder_path}/qualifying*',multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Data cleaning(Add,remove,rename columns)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

qualifying_selected_df = qualifying_input_df.withColumnRenamed("qualifyId","qualify_id")\
                                            .withColumnRenamed("raceId","race_id")\
                                            .withColumnRenamed("driverId","driver_Id")\
                                            .withColumnRenamed("constructorId","constructor_id")\
                                            .withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the data to datalake in parquet format

# COMMAND ----------

qualifying_selected_df.write.mode("overwrite").parquet(f'{processed_folder_path}/qualifying')


