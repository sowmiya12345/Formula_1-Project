# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

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
# MAGIC ##### Step 1 - Read the csv file using spark dataframe reader 

# COMMAND ----------

# Define the schema for the csv file.
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType
circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),\
                              StructField("circuitRef", StringType(), True),\
                              StructField("name", StringType(), True),\
                              StructField("location", StringType(), True),\
                              StructField("country", StringType(), True),\
                              StructField("lat", DoubleType(), True),\
                              StructField("long", DoubleType(), True),\
                              StructField("alt", IntegerType(), True),\
                              StructField("url", StringType(), True),\
                              ])

circuits_input_df = spark.read \
.option("header",True) \
.schema(circuits_schema)\
.csv(f'{input_folder_path}/circuits.csv')


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only required columns from the input csv file

# COMMAND ----------

circuits_selected_df = circuits_input_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),
                       col("long"),col("alt"))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Data Cleaning Process/Data transformation(Renamed Columns & Added ingestion_date column)

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_final_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
                                  .withColumnRenamed("circuitRef","circuit_ref")\
                                  .withColumnRenamed("lat","latitude")\
                                  .withColumnRenamed("long","longitude")\
                                  .withColumnRenamed("alt","altitude")\
                                  .withColumn("data_source",lit(data_source)) #Added data source value passed at run time to as column to df.
                                                                    
circuits_final_df = add_ingestion_date(circuits_final_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake in parquet format

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")


