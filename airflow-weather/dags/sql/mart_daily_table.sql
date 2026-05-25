CREATE OR REPLACE TABLE `weather-project-497009.data_mart.mart_daily_table` AS
SELECT d.city_id,
       city,
       timezone,
       DATE(weather_timestamp) as weather_date,
       ROUND(AVG(temperature_2m) ,2) as average_temperature,
       MIN(temperature_2m) as min_temperature,
       MAX(temperature_2m) as max_temperature,
       COUNT(*) as records_count,
       MAX(ingested_at) as latest_ingested_at
FROM `weather-project-497009.gold.fact_weather` as f
JOIN `weather-project-497009.gold.dim_city` as d
ON d.city_id=f.city_id
GROUP BY city_id, city, timezone, weather_date
ORDER BY city_id, weather_date DESC;

