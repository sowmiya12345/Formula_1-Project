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
# MAGIC ##### Step -1 Ingest constructors json data to Dataframe(Single line json)

# COMMAND ----------

# Define the schema for the single line json file.
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

constructor_schema = StructType(fields = [StructField("constructorId", IntegerType(), False),\
                              StructField("constructorRef", StringType(), True),\
                              StructField("name", StringType(), True),\
                              StructField("nationality", StringType(), True),\
                              StructField("url", StringType(), True)
                              ])

constructor_input_df = spark.read \
.schema(constructor_schema) \
.json(f'{input_folder_path}/constructors.json')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Data cleaning(Removal & Addition of columns)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp
constructor_selected_df = constructor_input_df.drop(col("url"))\
                                           .withColumnRenamed("constructorId","constructor_id")\
                                           .withColumnRenamed("constructorRef","constructor_ref")\
                                           .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Writing result in parquet format

# COMMAND ----------

constructor_selected_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructor")


# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/constructor")




