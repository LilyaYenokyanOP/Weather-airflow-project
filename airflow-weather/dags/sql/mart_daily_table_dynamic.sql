-- Same as mart_daily_table.sql: gold -> data mart.
-- Used by dynamic_task_mapping DAG.

CREATE OR REPLACE TABLE `weather-project-497009.data_mart.mart_daily_table_dynamic` AS
SELECT d.city_id,
       city,
       timezone,
       DATE(weather_timestamp) AS weather_date,
       ROUND(AVG(temperature_2m), 2) AS average_temperature,
       MIN(temperature_2m) AS min_temperature,
       MAX(temperature_2m) AS max_temperature,
       COUNT(*) AS records_count,
       MAX(ingested_at) AS latest_ingested_at
FROM `weather-project-497009.gold.fact_weather_dynamic` AS f
JOIN `weather-project-497009.gold.dim_city_dynamic` AS d
  ON d.city_id = f.city_id
GROUP BY city_id, city, timezone, weather_date
ORDER BY city_id, weather_date DESC;
