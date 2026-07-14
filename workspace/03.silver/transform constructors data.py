# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform constructors data
# MAGIC 1. Read bronze constructors table
# MAGIC 2. Keep only columns required for analytics(Drop url column)
# MAGIC 3. Standardize columns using snake_case (constructorId - constructor_id)
# MAGIC 4. Rename columns to make them more meaningful (name - constructor_name)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of nationality to Title case
# MAGIC 7. Write the transformed data to silver constructors table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.constructors'
silver_table = f'{catalog_name}.{silver_schema}.constructors'

# COMMAND ----------

bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read bronze constructors table

# COMMAND ----------

#constructors_df = spark.table(bronze_table)
constructors_df = (spark.table(bronze_table).filter((F.col('batch_id') == v_batch_id)))

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Keep only columns required for analytics(Drop url column)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4. Standardize column names 

# COMMAND ----------

#constructors_renamed_df = (constructors_dropped_df
#                           .withColumnRenamed('constructorId', 'constructor_id')
#                           .withColumnRenamed('name', 'constructor_name')
#)

# COMMAND ----------

constructors_renamed_df = (constructors_dropped_df.withColumnsRenamed({
                                                    'constructorId': 'constructor_id',
                                                    'name': 'constructor_name'})
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Remove duplicate records

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.distinct()

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates()

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(['constructor_id'])

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6. Transform values of nationality to Title case

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_final_df = constructors_distinct_df.withColumn('nationality', F.initcap(F.col('nationality')))

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7. Write the transformed data to silver races table

# COMMAND ----------

constructors_final_df.columns

# COMMAND ----------

write_to_silver(
input_df = constructors_final_df,
target_table = silver_table,
merge_condition = 't.constructor_id = s.constructor_id',
columns_to_update = [
    'constructor_id',
    'constructor_name',
    'nationality',
    'ingested_timestamp',
    'source_file',
    'batch_id'
]
)

# COMMAND ----------

'''(
    constructors_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)
'''

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_incr.silver.constructors