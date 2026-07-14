# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform results data
# MAGIC 1. Read bronze results table
# MAGIC 2. Keep only columns required for analytics(Drop url column)
# MAGIC 3. Standardize columns using snake_case (constructorId - constructor_id, driverId - driver_id, raceName - race_name, positionText -finish_position_text)
# MAGIC 4. Rename columns to make them more meaningful (race - race_date, grid - grid_position, laps - completed_laps, number - car_number, position - finsh_position )
# MAGIC 5. Filter out rows where season, round, constructor_id, driver_id is null(business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of race_name to Title case
# MAGIC 8. Write the transformed data to silver results table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.sprints'
silver_table = f'{catalog_name}.{silver_schema}.sprints'

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1-4. Read bronze constructors table, select only required columns and standardize column names

# COMMAND ----------

sprints_df = (
    spark.table(bronze_table).filter((F.col('batch_id') == v_batch_id))
    .select(
        "date",
        "raceName",
        "round",
        "season",
        "constructorId",
        "driverId",
        "grid",
        "laps",
        "number",
        "points",
        "position",
        "positionText",
        "status",
        "ingested_timestamp",
        "source_file",
        "batch_id"
    )
    .withColumnsRenamed(
        {
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            " positionText": "finish_position_text",
            "race": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "finish_position",
        }
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5 & 6. Apply Data Quality Checks

# COMMAND ----------

sprints_valid_df = sprints_df.filter(
    F.col("season").isNotNull()
    & F.col("round").isNotNull()
    & F.col("constructor_id").isNotNull()
    & F.col("driver_id").isNotNull()
).dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Transform values of race_name to Title case

# COMMAND ----------

sprints_final_df = sprints_valid_df.withColumn('race_name', F.initcap(F.col('race_name')))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver results table

# COMMAND ----------

sprints_final_df.columns

# COMMAND ----------

write_to_silver(
input_df = sprints_final_df,
target_table = silver_table,
merge_condition = 't.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id',
columns_to_update = [
    'date',
    'race_name',
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
    'ingested_timestamp',
    'source_file',
    'batch_id'
]
)

# COMMAND ----------

'''
(
    results_final_df
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
# MAGIC SELECT *FROM formula1_incr.silver.sprints