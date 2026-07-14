# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform races data
# MAGIC 1. Read bronze races table
# MAGIC 2. Keep only columns required for analytics(Drop url column)
# MAGIC 3. Standardize columns using snake_case (raceName- race_name. circuitId- circuit_id)
# MAGIC 4. Rename columns to make them more meaningful (date- race_date)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of race_name and locality to Title case
# MAGIC 7. Write the transformed data to silver races table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/03.silver-helpers

# COMMAND ----------

silver_table = f'{catalog_name}.{silver_schema}.races'
bronze_table = f'{catalog_name}.{bronze_schema}.races'

# COMMAND ----------

silver_table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read bronze circuits table

# COMMAND ----------

races_df = (spark.table(bronze_table).filter((F.col('batch_id') == v_batch_id)))
#races_df = spark.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Keep only columns required for analytics(Drop url column)

# COMMAND ----------

races_df.columns

# COMMAND ----------

#races_selected_df = races_df.select(
#'season',
# 'round',
# 'raceName',
#'date',
# 'circuitId',
# 'ingested_timestamp',
#'source_file'
#)

# COMMAND ----------

from pyspark.sql import functions as F

races_selected_df = races_df.select(
F.col('season'),
F.col('round'),
F.col('raceName'),
F.col('date'),
F.col('circuitId'),
F.col('ingested_timestamp'),
F.col('source_file'),
F.col('batch_id')
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4. Standardize column names 

# COMMAND ----------

races_renamed_df = (races_selected_df.withColumnsRenamed({
    'raceName':'race_name',
    'circuitId':'circuit_id',
    'date':'race_date'
})
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Remove duplicate records

# COMMAND ----------

races_distinct_df = races_renamed_df.distinct()

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates()

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(['season', 'round'])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6. Transform values of race_name to Title case

# COMMAND ----------

races_final_df = races_distinct_df.withColumn('race_name', F.initcap(F.col('race_name')))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7. Write the transformed data to silver races table

# COMMAND ----------

races_final_df.columns

# COMMAND ----------

write_to_silver(
input_df = races_final_df,
target_table = silver_table,
merge_condition = 't.season = s.season AND t.round = s.round',
columns_to_update = [
    'season',
    'round',
    'race_name',
    'race_date',
    'circuit_id',
    'ingested_timestamp',
    'source_file',
    'batch_id'
]
)

# COMMAND ----------

'''
(
    races_final_df
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
# MAGIC SELECT * FROM formula1_incr.silver.races