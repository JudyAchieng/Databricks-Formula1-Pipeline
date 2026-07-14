# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Circuits data
# MAGIC 1. Read bronze circuitds table
# MAGIC 2. Keep only columns required for analytics(Drop url column)
# MAGIC 3. Standardize columns using snake_case (circuitName- circuit_name. circuitId- circuit_id)
# MAGIC 4. Rename columns to make them more meaningful (lat- latitude, long-longitude)
# MAGIC 5. Filter out rows where circuit_id is nul(business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of circuit_name and locality to Title case
# MAGIC 8. Write the transformed data to silver circuits table
# MAGIC
# MAGIC Below changes are required to implement Incremental Load Processing
# MAGIC 1. Accept batch_id as a parameter to the notebook
# MAGIC 2. Process data for only the batch_id being passed in (i.e., filter reading from bronze using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timestamp and batch_id to the silver table. 
# MAGIC 4. Merge the processed data to the silver table
# MAGIC  - created_timestamp should only be populated at the time of inserting/ creating the record. It should not be  updated during the merge update.
# MAGIC  - Ensure that we are not overwriting the data in silver table by older bronze data (re-run scenario)

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/03.silver-helpers

# COMMAND ----------


bronze_table = f'{catalog_name}.{bronze_schema}.circuits'
silver_table = f'{catalog_name}.{silver_schema}.circuits'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read bronze circuits table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter((F.col('batch_id') == v_batch_id))
)

# COMMAND ----------

#circuits_df = spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Keep only columns required for analytics(Drop url column)

# COMMAND ----------

#circuits_selected_df = circuits_df.select(
#    'circuitId',
#    'circuitName',
#   'lat',
#   'long',
#   'locality',
#   'country',
#   'ingested_timestamp',
#   'source_file'
#)

# COMMAND ----------

from pyspark.sql import functions as F

circuits_selected_df = circuits_df.select(
    F.col('circuitId'),
    F.col('circuitName'),
    F.col('lat'),
    F.col('long'),
    F.col('locality'),
    F.col('country'),
    F.col('ingested_timestamp'),
    F.col('source_file'),
    F.col('batch_id')
)

# COMMAND ----------

circuits_selected_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4. Standardize column names

# COMMAND ----------

circuits_renamed_df = (circuits_selected_df
                       .withColumnRenamed('circuitId', 'circuit_id')
                       .withColumnRenamed('circuitName', 'circuit_name')
                       .withColumnRenamed('lat', 'latitude')
                       .withColumnRenamed('long', 'longitude')
)

# COMMAND ----------

circuits_renamed_df = (circuits_selected_df.withColumnsRenamed(
    {'circuitId': 'circuit_id',
     'circuitName': 'circuit_name',
     'lat': 'latitude',
     'long': 'longitude',
     
     }
)
)                 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Filter out rows where circuit_id is nul(business key validation)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.na.drop()

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

#circuits_valid_df = circuits_renamed_df.filter('circuit_id is not null')
circuits_valid_df = circuits_renamed_df.filter('circuit_id IS NOT NULL')


# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(F.col('circuit_id').isNotNull())

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(circuits_renamed_df.circuit_id.isNotNull())

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6. Remove duplicate records

# COMMAND ----------

#circuits_distinct_df = circuits_valid_df.distinct()


# COMMAND ----------

#circuits_distinct_df = circuits_valid_df.dropDuplicates()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(['circuit_id'])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7. Transform values of circuit_name and locality to Title case

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn('circuit_name', F.initcap(F.col('circuit_name')))
    .withColumn('locality', F.initcap(F.col('locality')))
    )

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8. Write the transformed data to silver circuits table

# COMMAND ----------

write_to_silver(
input_df = circuits_final_df,
target_table = silver_table,
merge_condition = 't.circuit_id = s.circuit_id'  ,
columns_to_update = [
    'circuit_name',
    'latitude',
    'longitude',
    'locality',
    'country',
    'ingested_timestamp',
    'source_file',
    'batch_id',
]
)

# COMMAND ----------

#circuits_final_df = (
#    circuits_final_df
#   .withColumn('current_timestamp', F.current_timestamp())
#    .withColumn('updated_timestamp', F.current_timestamp())
#)

# COMMAND ----------

"""
if not spark.catalog.tableExists(silver_table):
    (
        circuits_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable(silver_table)
    )
else:

    from delta.tables import DeltaTable

    delta_table = DeltaTable.forName(spark, silver_table)
    (
        delta_table.alias("t")
        .merge(
            circuits_final_df.alias('s'),
            't.circuit_id = s.circuit_id'
        )
        .whenMatchedUpdate(
            condition= 's.batch_id >= t.batch_id',
            set ={
                'circuit_name': 's.circuit_name',
                'latitude': 's.latitude',
                'longitude': 's.longitude',
                'locality': 's.locality',
                'country': 's.country',
                'ingested_timestamp': 's.ingested_timestamp',
                'source_file': 's.source_file',
                'batch_id': 's.batch_id',
                'updated_timestamp': 's.updated_timestamp'
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
        )
"""

# COMMAND ----------

#(
#    circuits_final_df
#    .write
#    .mode('overwrite')
#    .format('delta')
#    .saveAsTable(silver_table)
#)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_incr.silver.circuits

# COMMAND ----------

spark.table(silver_table).columns