-- creating the dim_city table
CREATE TABLE IF NOT EXISTS `weather-project-497009.gold.dim_city` AS
SELECT DISTINCT   TO_HEX(MD5(CONCAT(city, CAST(latitude AS STRING), CAST(longitude AS STRING), timezone))) AS city_id,
                                    city,
                                    latitude,
                                    longitude,
                                        timezone
FROM `weather-project-497009.silver.weather_cleaned`;


--creating fact_weather table
CREATE TABLE IF NOT EXISTS `weather-project-497009.gold.fact_weather` 
PARTITION BY DATE(ingested_at)
CLUSTER BY city_id, batch_id AS
SELECT
 d.city_id,
 w.weather_timestamp,
 w.temperature_2m,
 w.batch_id,
 w.ingested_at
FROM `weather-project-497009.silver.weather_cleaned` as w
JOIN `weather-project-497009.gold.dim_city` as d
ON w.city = d.city
AND w.latitude = d.latitude
AND w.longitude = d.longitude
AND w.timezone = d.timezone;
