-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Driver Standings
-- MAGIC #### Sources
-- MAGIC 1. fact_session_results
-- MAGIC 2. dim_drivers
-- MAGIC
-- MAGIC #### Output Columns
-- MAGIC 1. season
-- MAGIC 2. driver id
-- MAGIC 3. driver name
-- MAGIC 4. nationality
-- MAGIC 5. race starts
-- MAGIC 6. total points
-- MAGIC 7. number of wins
-- MAGIC 8. number of podiums
-- MAGIC 10. standing position
-- MAGIC

-- COMMAND ----------

SELECT * FROM formula1.gold.dim_drivers

-- COMMAND ----------

SELECT * FROM formula1.gold.facts_session_results

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.v_driver_standings
AS
WITH driver_standing
AS
(SELECT r.season,
r.driver_id,
d.driver_name,
d.nationality,
COUNT(*) AS race_starts,
SUM(r.points) AS total_points,
COUNT_IF(is_win) AS number_of_wins,
COUNT_IF(is_podium) AS number_of_podiums
FROM formula1.gold.facts_session_results r
JOIN formula1.gold.dim_drivers d
    ON r.driver_id = d.driver_id
GROUP BY r.season,
r.driver_id,
d.driver_name,
d.nationality)

SELECT 
season,
driver_id,
driver_name,
nationality,
RANK() OVER(PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standings,
race_starts,
total_points,
number_of_wins,
number_of_podiums
FROM driver_standing


-- COMMAND ----------

SELECT * FROM formula1.gold.v_driver_standings WHERE season = 2025