-- Databricks notebook source
SELECT * FROM formula1.gold.v_driver_standings

-- COMMAND ----------

WITH driver_score
AS
(
    SELECT driver_name,
        SUM(race_starts) AS race_starts,
        SUM(number_of_wins) AS total_wins,
        SUM(number_of_podiums) AS total_podiums,
        SUM(CASE WHEN standings = 1 THEN 1 ELSE 0 END) AS total_championship
    FROM formula1.gold.v_driver_standings
        GROUP BY driver_name
        HAVING total_championship > 0
)

SELECT driver_name,
    race_starts,
    total_wins,
    total_podiums,
    total_championship,
    (total_championship*100) + (total_wins*10) +(total_podiums*3) AS greatness_score
FROM driver_score
ORDER BY greatness_score DESC