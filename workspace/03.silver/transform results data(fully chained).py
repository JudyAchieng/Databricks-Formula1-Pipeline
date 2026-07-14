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

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.results'
silver_table = f'{catalog_name}.{silver_schema}.results'

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1-7. Read, transform, perform data quality checks

# COMMAND ----------

results_df = (
    spark.table(bronze_table)
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
    .filter(
        F.col("season").isNotNull()
        & F.col("round").isNotNull()
        & F.col("constructor_id").isNotNull()
        & F.col("driver_id").isNotNull()
    )
    .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
    .withColumn("race_name", F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver results table

# COMMAND ----------

(
    results_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM formula1.bronze.results