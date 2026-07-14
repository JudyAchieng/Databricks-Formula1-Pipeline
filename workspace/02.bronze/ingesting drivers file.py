# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting drivers.json file
# MAGIC 1. Read the file using spark dataframe reader API 
# MAGIC 2. Define metadata
# MAGIC 3. Add Metadata columns
# MAGIC -     Source File
# MAGIC -     Ingestion Timestamps
# MAGIC 4. Write to bronze delta table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/02.bronze-helpers

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/drivers.json'
table_name = f'{catalog_name}.{bronze_schema}.drivers'

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1: Read the file using spark dataframe reader API 

# COMMAND ----------

#Define schema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType()),
  
])

# COMMAND ----------

drivers_df = (
    spark.read.format('json')
    .option('mode', 'FAILFAST')
    .schema(drivers_schema)
    .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Add Metadata columns
# MAGIC -     Source File
# MAGIC -     Ingestion Timestamps

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3. Write to bronze delta table

# COMMAND ----------

#(drivers_final_df
#.write
#.format('delta')
#.mode('overwrite')
#.saveAsTable(table_name)
#)

# COMMAND ----------

write_to_bronze(
    input_df = drivers_final_df,
    target_table = table_name,
    batch_id = v_batch_id)

# COMMAND ----------

display(spark.table(table_name))