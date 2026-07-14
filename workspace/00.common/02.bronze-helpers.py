# Databricks notebook source
# Helper functions for adding metadata(ingestion_timestamp and source_file)

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df.withColumn('ingested_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
           )

# COMMAND ----------

def write_to_bronze(input_df, target_table, batch_id):

    final_df = input_df.withColumn('batch_id', F.lit(batch_id))

    (
    final_df
    .write
    .format('delta')
    .mode('overwrite')
    .partitionBy('batch_id')
    .option('replaceWhere', f"batch_id = '{batch_id}'")
    .saveAsTable(table_name)
)