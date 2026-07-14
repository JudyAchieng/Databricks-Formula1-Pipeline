# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform drivers data
# MAGIC 1. Read bronze drivers table
# MAGIC 2. Keep only columns required for analytics(Drop url column)
# MAGIC 3. Standardize columns using snake_case (driverId - driver_id, dateOfbirth - date_of_birth)
# MAGIC 4. Concatenate name.givenName and name.FamilyName to create a new column called driver_name and transform the value to Title Case
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of nationality to Title case
# MAGIC 7. Write the transformed data to silver drivers table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.drivers'
silver_table = f'{catalog_name}.{silver_schema}.drivers'

# COMMAND ----------

bronze_table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read bronze constructors table

# COMMAND ----------

#drivers_df = spark.table(bronze_table)
drivers_df = (spark.table(bronze_table).filter((F.col('batch_id') == v_batch_id)))

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Keep only columns required for analytics(Drop url column)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Standardize columns using snake_case (driverId - driver_id, dateOfbirth - date_of_birth)

# COMMAND ----------

drivers_renamed_df = drivers_dropped_df.withColumnsRenamed({
    'driverId':'driver_id',
    'dateOfbirth':'date_of_birth'
        })


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4. Concatenate name.givenName and name.FamilyName to create a new column called driver_name and transform the value to Title Case

# COMMAND ----------

drivers_concatenated_df = drivers_renamed_df.withColumn('driver_name', F.initcap(F.concat_ws(' ', F.col('name.givenName'), F.col('name.familyName')))).drop('name')

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Remove duplicate records

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(['driver_id'])

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6. Transform values of nationality to Title case

# COMMAND ----------

drivers_final_df = drivers_distinct_df.withColumn('nationality', F.initcap(F.col('nationality')))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7. Write the transformed data to silver races table

# COMMAND ----------

drivers_final_df.columns

# COMMAND ----------

write_to_silver(
input_df = drivers_final_df,
target_table = silver_table,
merge_condition = 't.driver_id = s.driver_id'  ,
columns_to_update = [
    'driver_id',
    'date_of_birth',
    'nationality',
    'ingested_timestamp',
    'source_file',
    'batch_id',
    'driver_name'
]
)

# COMMAND ----------

'''
(
    drivers_final_df
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
# MAGIC SELECT *FROM formula1_incr.silver.drivers