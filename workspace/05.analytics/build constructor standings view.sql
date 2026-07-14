-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Constructor Standings
-- MAGIC #### Sources
-- MAGIC 1. fact_session_results
-- MAGIC 2. dim_constructors
-- MAGIC
-- MAGIC #### Output Columns
-- MAGIC 1. season
-- MAGIC 2. constructor id
-- MAGIC 3. constructor name
-- MAGIC 4. nationality
-- MAGIC 5. race starts
-- MAGIC 6. total points
-- MAGIC 7. number of wins
-- MAGIC 8. number of podiums
-- MAGIC 10. standing position
-- MAGIC

-- COMMAND ----------

SELECT * FROM formula1.gold.dim_constructors

-- COMMAND ----------

SELECT * FROM formula1.gold.facts_session_results

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.v_constructor_standings
AS
WITH constructor_standings
AS
(
    SELECT 
    r.season,
    r.constructor_id,
    c.constructor_name,
    c.nationality,
    COUNT(*) AS race_starts,
    SUM(points) AS total_points,
    COUNT_IF(is_win) AS number_of_wins,
    COUNT_IF(is_podium) AS number_of_podiums
    FROM formula1.gold.dim_constructors c
    JOIN formula1.gold.facts_session_results r
    ON c.constructor_id = r.constructor_id
    GROUP BY r.season,
    r.constructor_id,
    c.constructor_name,
    c.nationality

)

SELECT 
season,
constructor_id,
constructor_name,
nationality,
race_starts,
total_points,
number_of_wins,
number_of_podiums,
RANK () OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing_position
FROM constructor_standings

-- COMMAND ----------

SELECT * FROM formula1.gold.v_constructor_standings WHERE season = 2025