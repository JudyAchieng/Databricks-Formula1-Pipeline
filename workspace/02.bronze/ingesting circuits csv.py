# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting circuits.csv file
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

v_batch_id

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

catalog_name

# COMMAND ----------

bronze_schema

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/circuits.csv'
table_name = f'{catalog_name}.{bronze_schema}.circuits'

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1: Read the file using spark dataframe reader API 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId',   StringType()),
    StructField('url',         StringType()),
    StructField('circuitName', StringType()),
    StructField('lat',         DoubleType()),
    StructField('long',        DoubleType()),
    StructField('locality',    StringType()),
    StructField('country',     StringType()),
])

# COMMAND ----------

circuits_df = (
    spark.read.format('csv')
    .option('header', 'true')
    .option('mode', 'FAILFAST')
 #   .option('inferSchema', 'true')
    .schema(circuits_schema)    
    .load(source_file)
    )

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Add Metadata columns
# MAGIC -     Source File
# MAGIC -     Ingestion Timestamps

# COMMAND ----------

#from pyspark.sql import functions as F

#circuits_final_df = (circuits_df
#                    .withColumn('ingested_timestamp', F.current_timestamp())
#                    .withColumn('source_file', F.col('_metadata.file_path'))
#)

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

from pyspark.sql.functions import *

circuits_df2 = (circuits_df
                     .withColumn('ingested_timestamp', current_timestamp())
                     .withColumn('source_file', col('_metadata.file_path'))
)

# COMMAND ----------

display(circuits_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3. Write to bronze delta table

# COMMAND ----------

#circuits_final_df = circuits_final_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

#(
#   circuits_final_df
#    .write
#   .format('delta')
#   .mode('overwrite')
#    .partitionBy('batch_id')
#    .option('replaceWhere', f"batch_id = '{v_batch_id}'")
#    .saveAsTable(table_name)
#)

# COMMAND ----------

write_to_bronze(
    input_df = circuits_final_df,
    target_table = table_name,
    batch_id = v_batch_id)

# COMMAND ----------

# MAGIC %sql SELECT * FROM formula1_incr.bronze.circuits 

# COMMAND ----------

display(spark.table(table_name))