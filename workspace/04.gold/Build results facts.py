# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC 1. Read silver `results` table
# MAGIC 2. Read silver `sprints` table
# MAGIC 3. Add new column `session_type` with values `RACE` or `SPRINT`
# MAGIC 4. UNION `results` and `sprints`
# MAGIC 5. Derive additional columns
# MAGIC      - is_win -> Indicates that the driver own the race
# MAGIC      - is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC     - has_points -> Indicates that the driver has scored points
# MAGIC 6. Write the transformed data to gold `fact_session_results` table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config
# MAGIC

# COMMAND ----------

# MAGIC %run ../00.common/04.gold-helpers

# COMMAND ----------

target_table = f'{catalog_name}.{gold_schema}.facts_session_results'

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables

# COMMAND ----------

results_df =(
    spark.table(f'{catalog_name}.{silver_schema}.results')
    .filter((F.col('batch_id') == v_batch_id))
    .withColumn('session_type', F.lit('RACE'))
    .drop('race_name','race_date','ingested_timestamp','source_file', 'batch_id', 'created_timestamp', 'updated_timestamp')
)

sprints_df =(
    spark.table(f'{catalog_name}.{silver_schema}.sprints')
    .filter((F.col('batch_id') == v_batch_id))
    .withColumn('session_type', F.lit('SPRINT'))
    .drop('race_name','race_date','ingested_timestamp','source_file', 'batch_id', 'created_timestamp', 'updated_timestamp')
)

# COMMAND ----------

display(results_df)
display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - UNION `results` and `sprints`

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

display(results_sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add dervied columns
# MAGIC 1. is_win -> Indicates that the driver own the race
# MAGIC 2. is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC 3. has_points -> Indicates that the driver has scored points

# COMMAND ----------

facts_session_results_df = (
    results_sprints_df
    .withColumn('is_win', F.col("finish_position") == 1)
    .withColumn('is_podium', F.col("finish_position").between(1,3))
    .withColumn('has_points', F.col("points") > 0)
)

# COMMAND ----------

display(facts_session_results_df.filter('season =2025'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the transformed data to the `gold` `fact_session_results` table

# COMMAND ----------

facts_session_results_df.columns

# COMMAND ----------

write_to_gold(
input_df = facts_session_results_df,
target_table = target_table,
merge_condition = 't.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id AND t.session_type = s.session_type',
columns_to_update = [
    'date',
    'round',
    'season',
    'constructor_id',
    'driver_id',
    'grid_position',
    'completed_laps',
    'car_number',
    'points',
    'finish_position',
    'positionText',
    'status',
    'current_timestamp',
    'session_type',
    'is_win',
    'is_podium',
    'has_points'
]
)

# COMMAND ----------

'''
(
    facts_session_results_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)
'''

# COMMAND ----------

display(spark.table(target_table))