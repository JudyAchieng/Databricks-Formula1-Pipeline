# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting sprints.json file- folder
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

source_file = f'{landing_folder_path}/{v_batch_id}/sprints'
table_name = f'{catalog_name}.{bronze_schema}.sprints'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1: Read the file using spark dataframe reader API 

# COMMAND ----------

#Define schema
from pyspark.sql.types import *

sprints_schema = StructType([
    StructField('date', StringType()),
    StructField('raceName', StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('url', StringType()),
    StructField('constructorId', StringType()),
    StructField('driverId', StringType()),
    StructField('grid', StringType()),
    StructField('laps', StringType()),
    StructField('number', IntegerType()),
    StructField('points', FloatType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('status', StringType())
])


# COMMAND ----------

sprints_df = (
    spark.read.format('json')
    .option('mode', 'FAILFAST')
    .option('multiLine', True)
    .schema(sprints_schema)
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Add Metadata columns
# MAGIC -     Source File
# MAGIC -     Ingestion Timestamps

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3. Write to bronze delta table

# COMMAND ----------

write_to_bronze(
    input_df = sprints_final_df,
    target_table = table_name,
    batch_id = v_batch_id)

# COMMAND ----------

#(sprints_final_df
#.write
#.format('delta')
#.mode('overwrite')
#.saveAsTable(table_name)
#)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, COUNT(*)
# MAGIC FROM formula1_incr.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season 