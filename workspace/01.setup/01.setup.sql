-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Setup environment for formula 1 icremental loading project
-- MAGIC 1. Create external location databricks_training_ext_dl_formula1_incr
-- MAGIC 2. Create unity catalog formula1_incr
-- MAGIC 3. Create schemas landing, bronze, silver and gold
-- MAGIC 4. Create volume files in the landing schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Access cloud storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create external location databricks_training_ext_dl_formula1

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_training_ext_dl_formula1_incr
    URL 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databricks-training-sc`)
    COMMENT 'External location of the formula1 container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create unity catalog formula1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG  IF NOT EXISTS  formula1_incr
      MANAGED LOCATION 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/'
      COMMENT 'This is the main catalog for the formula1 project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Create schemas landing, bronze, silver and gold

-- COMMAND ----------

CREATE SCHEMA  IF NOT EXISTS  formula1_incr.landing;
CREATE SCHEMA  IF NOT EXISTS  formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/bronze';
CREATE SCHEMA  IF NOT EXISTS  formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/silver';
CREATE SCHEMA  IF NOT EXISTS  formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/gold';

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG formula1_incr;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Create volume files in the landing schema

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.files
    LOCATION 'abfss://formula1-incr@databrickstrainingextdl.dfs.core.windows.net/landing';

 

-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/formula1_incr/landing/files'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC