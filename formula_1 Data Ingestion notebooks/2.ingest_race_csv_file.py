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
# MAGIC ##### Step 1 - Read data from races csv file into a dataframe after defining the schema

# COMMAND ----------

# Define the schema for the csv file.
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType
races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),\
                              StructField("year", IntegerType(), True),\
                              StructField("round", IntegerType(), True),\
                              StructField("circuitId", IntegerType(), True),\
                              StructField("name", StringType(), True),\
                              StructField("date", StringType(), True),\
                              StructField("time", StringType(), True),\
                              StructField("url", StringType(), True),\
                              ])

races_input_df = spark.read \
.option("header",True) \
.schema(races_schema)\
.csv(f'{input_folder_path}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Data cleaning/Data Transformation(Renaming,Adding & Concatenating columns)

# COMMAND ----------

from pyspark.sql.functions import concat,lit,to_timestamp,col,current_timestamp
races_transformed_df = races_input_df.withColumnRenamed("raceId","race_id")\
                                     .withColumnRenamed("year","race_year")\
                                     .withColumnRenamed("circuitId","circuit_id")\
                                     .withColumn("race_timestamp",to_timestamp(concat("date",lit(" "),"time"),'yyyy-MM-dd HH:mm:ss'))\
                                     .withColumn("ingestion_date",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the required columns

# COMMAND ----------

races_selected_df =races_transformed_df.select(col("race_id"),col("race_year"),\
                                                col("round"),col("circuit_id"),\
                                                col("name"),col("race_timestamp"),\
                                                col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the result of df in parquet format

# COMMAND ----------

races_selected_df.write.mode("overwrite").parquet("/FileStore/tables/processed/races")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Parition data by race_year and write the result in parquet format

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")


