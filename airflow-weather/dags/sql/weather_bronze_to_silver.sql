--first uery will check nulls, texts, and types 
-- CREATE OR REPLACE TABLE silver.weather_cleaned
--   AS
--   SELECT
--      TRIM(city) as city,
--      CAST(latitude AS FLOAT64) AS latitude,
--      CAST(longitude AS FLOAT64) AS longitude,
--      timezone,

--      SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) as weather_timestamp,
--      CAST(temperature_2m AS FLOAT64) AS temperature_2m,

--      batch_id,
--      ingested_at
     
-- FROM `weather-project-4567.bronze.weather_raw_flattened_partitioned`
-- WHERE city is null and temperature_2m is not NULL



-- -- check duplicates
-- SELECT city, timestamp
-- FROM `weather-project-4567.bronze.weather_raw_flattened_partitioned`
-- GROUP BY city, timestamp
-- HAVING COUNT(*) > 1

-- --deduplicated data
-- SELECT * 
-- FROM (
--     SELECT
--         *,
--         ROW_NUMBER() OVER (
--             PARTITION BY city, `timestamp`
--             ORDER BY ingested_at DESC
--         ) AS rn
--     FROM `weather-project-4567.bronze.weather_raw_flattened_partitioned`
-- )
-- WHERE rn = 1 


-- optimized query 
CREATE TABLE IF NOT EXISTS `weather-project-497009.silver.weather_cleaned`
(
    city STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    timezone STRING,
    weather_timestamp TIMESTAMP,
    temperature_2m FLOAT64,
    batch_id STRING,
    ingested_at TIMESTAMP
)
PARTITION BY DATE(weather_timestamp)
CLUSTER BY city, batch_id;


INSERT INTO `weather-project-497009.silver.weather_cleaned`
WITH cleaned_data AS(
    SELECT 
    TRIM(city) as city,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        timezone,

        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) as weather_timestamp,
        CAST(temperature_2m AS FLOAT64) AS temperature_2m,

        batch_id,
        ingested_at
    FROM `weather-project-497009.bronze.weather_raw_flattened_partitioned`
    WHERE city is not null
    AND temperature_2m is not null
    AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) IS NOT NULL
),

deduplicated_data AS(
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY city, weather_timestamp
                ORDER BY ingested_at DESC
            ) AS rn
        FROM cleaned_data
    )
    WHERE rn = 1 
)

SELECT * FROM deduplicated_data;


     

