-- Same as create_gold_weather_tables.sql: silver -> gold dim + fact.
-- Used by dynamic_task_mapping DAG (separate file so you can change gold logic independently).

CREATE OR REPLACE TABLE `weather-project-497009.gold.dim_city_dynamic` AS
SELECT DISTINCT
    TO_HEX(
        MD5(
            CONCAT(
                city,
                CAST(latitude AS STRING),
                CAST(longitude AS STRING),
                timezone
            )
        )
    ) AS city_id,

    city,
    latitude,
    longitude,
    timezone

FROM `weather-project-497009.silver.weather_cleaned_dynamic_tasks`;


CREATE OR REPLACE TABLE `weather-project-497009.gold.fact_weather_dynamic`
PARTITION BY DATE(ingested_at)
CLUSTER BY city_id, batch_id AS

SELECT
    d.city_id,
    w.weather_timestamp,
    w.temperature_2m,
    w.batch_id,
    w.ingested_at

FROM `weather-project-497009.silver.weather_cleaned_dynamic_tasks` AS w

JOIN `weather-project-497009.gold.dim_city_dynamic` AS d
    ON w.city = d.city
    AND w.latitude = d.latitude
    AND w.longitude = d.longitude
    AND w.timezone = d.timezone;
