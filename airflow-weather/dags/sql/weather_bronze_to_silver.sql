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

-- MERGE STRUCTURE FOR SILVER AND GOLD TABLES
-- MERGE [INTO] target_name [[AS] alias]
-- USING source_name [[AS] alias]
-- ON merge_condition
-- { when_clause } +

-- when_clause ::= matched_clause | not_matched_by_target_clause | not_matched_by_source_clause

-- matched_clause ::= WHEN MATCHED [ AND search_condition ] THEN { merge_update_clause | merge_delete_clause }

-- not_matched_by_target_clause ::= WHEN NOT MATCHED [BY TARGET] [ AND search_condition ] THEN merge_insert_clause

-- not_matched_by_source_clause ::= WHEN NOT MATCHED BY SOURCE [ AND search_condition ] THEN { merge_update_clause | merge_delete_clause }

-- merge_condition ::= bool_expression

-- search_condition ::= bool_expression

-- merge_update_clause ::= UPDATE SET update_item [, update_item]*
-- update_item ::= column_name = expression

-- merge_delete_clause ::= DELETE

-- merge_insert_clause ::= INSERT [(column_1 [, ..., column_n ])] input

-- input ::= VALUES (expr_1 [, ..., expr_n ]) | ROW

-- expr ::= expression | DEFAULT


-- optimized query but inserts duplicates
-- CREATE TABLE IF NOT EXISTS `weather-project-497009.silver.weather_cleaned`
-- (
--     city STRING,
--     latitude FLOAT64,
--     longitude FLOAT64,
--     timezone STRING,
--     weather_timestamp TIMESTAMP,
--     temperature_2m FLOAT64,
--     batch_id STRING,
--     ingested_at TIMESTAMP
-- )
-- PARTITION BY DATE(weather_timestamp)
-- CLUSTER BY city, batch_id;


-- --look for other cases to silver table not insert intoits including duplicates
-- INSERT INTO `weather-project-497009.silver.weather_cleaned`
-- WITH cleaned_data AS(
--     SELECT 
--     TRIM(city) as city,
--         CAST(latitude AS FLOAT64) AS latitude,
--         CAST(longitude AS FLOAT64) AS longitude,
--         timezone,

--         SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) as weather_timestamp,
--         CAST(temperature_2m AS FLOAT64) AS temperature_2m,

--         batch_id,
--         ingested_at
--     FROM `weather-project-497009.bronze.weather_raw_flattened_partitioned`
--     WHERE city is not null
--     AND temperature_2m is not null
--     AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', `timestamp`) IS NOT NULL
-- ),

-- deduplicated_data AS(
--     SELECT * EXCEPT(rn)
--     FROM (
--         SELECT
--             *,
--             ROW_NUMBER() OVER (
--                 PARTITION BY city, weather_timestamp
--                 ORDER BY ingested_at DESC
--             ) AS rn
--         FROM cleaned_data
--     )
--     WHERE rn = 1 
-- )

-- SELECT * FROM deduplicated_data;



--hope tp not have duplicates inside silver table
CREATE OR REPLACE TABLE `weather-project-497009.silver.weather_cleaned`
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
    FROM `weather-project-497009.bronze.weather_raw_flattened_partitioned`
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
     

