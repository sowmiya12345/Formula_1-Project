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
# MAGIC ##### Step 1 - Ingest drivers json file to Dataframe(Single line nested json)

# COMMAND ----------

# Define the schema for the single line nested json file.
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

name_schema = StructType(fields = [StructField("forename", StringType(),False),\
                                   StructField("surname",StringType(),False)
                          ])
                        
                            
drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),\
                                          StructField("driverRef", StringType(), False),\
                                          StructField("number", IntegerType(), True),\
                                          StructField("code", StringType(), True),\
                                          StructField("name", name_schema),\
                                          StructField("dob", DateType(), True),\
                                          StructField("nationality", StringType(), True),\
                                          StructField("url",StringType(),True)
                              ])

constructor_input_df = spark.read \
.schema(drivers_schema) \
.json(f'{input_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Data Cleaning(Drop,Add,Remove,Concat Columns)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit,concat

drivers_selected_df = constructor_input_df.drop(col("url"))\
                                          .withColumnRenamed("driverId","driver_id")\
                                          .withColumnRenamed("driverRef","driver_ref")\
                                          .withColumn("name",concat("name.forename",lit(" "),"name.surname"))\
                                          .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 3 Write results to parquet file

# COMMAND ----------

drivers_selected_df.write.mode("overwrite").parquet(f'{processed_folder_path}/drivers')


