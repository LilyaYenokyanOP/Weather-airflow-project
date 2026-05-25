-- -- creating the dim_city table
-- CREATE TABLE IF NOT EXISTS `weather-project-497009.gold.dim_city` AS
-- SELECT DISTINCT   TO_HEX(MD5(CONCAT(city, CAST(latitude AS STRING), CAST(longitude AS STRING), timezone))) AS city_id,
--                                     city,
--                                     latitude,
--                                     longitude,
--                                         timezone
-- FROM `weather-project-497009.silver.weather_cleaned`;


-- --creating fact_weather table
-- CREATE TABLE IF NOT EXISTS `weather-project-497009.gold.fact_weather` 
-- PARTITION BY DATE(ingested_at)
-- CLUSTER BY city_id, batch_id AS
-- SELECT
--  d.city_id,
--  w.weather_timestamp,
--  w.temperature_2m,
--  w.batch_id,
--  w.ingested_at
-- FROM `weather-project-497009.silver.weather_cleaned` as w
-- JOIN `weather-project-497009.gold.dim_city` as d
-- ON w.city = d.city
-- AND w.latitude = d.latitude
-- AND w.longitude = d.longitude
-- AND w.timezone = d.timezone;




--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
-- Bronze  -> usually append or replace batch
-- Silver  -> can use CREATE OR REPLACE at first
-- Gold dim -> MERGE is useful
-- Gold fact -> MERGE is very useful
-- Mart    -> usually CREATE OR REPLACE

CREATE OR REPLACE TABLE `weather-project-497009.gold.dim_city` AS
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

FROM `weather-project-497009.silver.weather_cleaned`;


CREATE OR REPLACE TABLE `weather-project-497009.gold.fact_weather`
PARTITION BY DATE(ingested_at)
CLUSTER BY city_id, batch_id AS

SELECT
    d.city_id,
    w.weather_timestamp,
    w.temperature_2m,
    w.batch_id,
    w.ingested_at

FROM `weather-project-497009.silver.weather_cleaned` AS w

JOIN `weather-project-497009.gold.dim_city` AS d
    ON w.city = d.city
    AND w.latitude = d.latitude
    AND w.longitude = d.longitude
    AND w.timezone = d.timezone;



--MERGE STRUCTURE FOR FACT_WEATHER TABLE
-- MERGE `weather-project-497009.gold.fact_weather` AS target

-- USING (
--     SELECT
--         d.city_id,
--         w.weather_timestamp,
--         w.temperature_2m,
--         w.batch_id,
--         w.ingested_at

--     FROM `weather-project-497009.silver.weather_cleaned` w

--     JOIN `weather-project-497009.gold.dim_city` d
--         ON w.city = d.city
--         AND w.latitude = d.latitude
--         AND w.longitude = d.longitude
--         AND w.timezone = d.timezone
-- ) AS source

-- ON target.city_id = source.city_id
-- AND target.weather_timestamp = source.weather_timestamp

-- WHEN MATCHED THEN
--     UPDATE SET
--         temperature_2m = source.temperature_2m,
--         batch_id = source.batch_id,
--         ingested_at = source.ingested_at

-- WHEN NOT MATCHED THEN
--     INSERT (
--         city_id,
--         weather_timestamp,
--         temperature_2m,
--         batch_id,
--         ingested_at
--     )
--     VALUES (
--         source.city_id,
--         source.weather_timestamp,
--         source.temperature_2m,
--         source.batch_id,
--         source.ingested_at
--     );
