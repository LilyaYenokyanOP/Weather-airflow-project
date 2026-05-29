-- Dynamic-task-mapping DAG: reads bronze rows loaded into weather_raw_dynamic_tasks.
-- Logic matches weather_bronze_to_silver.sql; only the bronze table name differs.

CREATE OR REPLACE TABLE `weather-project-497009.silver.weather_cleaned_dynamic_tasks`
PARTITION BY DATE(weather_timestamp)
CLUSTER BY city, batch_id AS

WITH cleaned_data AS (
    SELECT
        TRIM(city) AS city,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        timezone,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) AS weather_timestamp,
        CAST(temperature_2m AS FLOAT64) AS temperature_2m,
        batch_id,
        ingested_at
    FROM `weather-project-497009.bronze.weather_raw_dynamic_tasks`
    WHERE city IS NOT NULL
      AND temperature_2m IS NOT NULL
      AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) IS NOT NULL
),

deduplicated_data AS (
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

SELECT *
FROM deduplicated_data;
