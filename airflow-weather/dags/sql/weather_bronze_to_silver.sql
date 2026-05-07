--first uery will check nulls, texts, and types 
CREATE OR REPLACE TABLE silver.weather_cleaned
  AS
  SELECT
     TRIM(city) as city,
     CAST(latitude AS FLOAT64) AS latitude,
     CAST(longitude AS FLOAT64) AS longitude,
     timezone,

     SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) as weather_timestamp,
     CAST(temperature_2m AS FLOAT64) AS temperature_2m,

     batch_id,
     ingested_at
     
FROM `weather-project-4567.bronze.weather_raw_flattened_partitioned`
WHERE city is null and temperature_2m is NULL



-- check duplicates
SELECT city, timestamp
FROM `weather-project-4567.bronze.weather_raw_flattened_partitioned`
GROUP BY city, timestamp
HAVING COUNT(*) > 1

--deduplicated data
SELECT * 
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, `timestamp`
            ORDER BY ingested_at DESC
        ) AS rn
    FROM `weather-project-4567.bronze.weather_raw_flattened_partitioned`
)
WHERE rn = 1 