# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting constructors.json file
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

source_file = f'{landing_folder_path}/{v_batch_id}/constructors.json'
table_name = f'{catalog_name}.{bronze_schema}.constructors'

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1: Read the file using spark dataframe reader API 

# COMMAND ----------

#Define schema
constructors_schema = "constructorId STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructors_df = (
    spark.read.format('json')
    .option('mode', 'FAILFAST')
    .schema(constructors_schema)
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Add Metadata columns
# MAGIC -     Source File
# MAGIC -     Ingestion Timestamps

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3. Write to bronze delta table

# COMMAND ----------

#(constructors_final_df
#.write
#.format('delta')
#.mode('overwrite')
#.saveAsTable(table_name)
#)

# COMMAND ----------

write_to_bronze(
    input_df = constructors_final_df,
    target_table = table_name,
    batch_id = v_batch_id)

# COMMAND ----------

display(spark.table(table_name))