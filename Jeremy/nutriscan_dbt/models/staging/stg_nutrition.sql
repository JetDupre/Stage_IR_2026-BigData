{{ config(materialized='view') }}

SELECT
    code                                    AS product_code,
    nutriscore_score,
    last_modified_t,
    CAST(list_filter(nutriments, x -> x.name = 'energy-kcal')[1]['100g'] AS DOUBLE)   AS energy_kcal,
    CAST(list_filter(nutriments, x -> x.name = 'fat')[1]['100g'] AS DOUBLE)           AS fat_100g,
    CAST(list_filter(nutriments, x -> x.name = 'saturated-fat')[1]['100g'] AS DOUBLE) AS saturated_fat_100g,
    CAST(list_filter(nutriments, x -> x.name = 'sugars')[1]['100g'] AS DOUBLE)        AS sugar_100g,
    CAST(list_filter(nutriments, x -> x.name = 'salt')[1]['100g'] AS DOUBLE)          AS salt_100g,
    CAST(list_filter(nutriments, x -> x.name = 'proteins')[1]['100g'] AS DOUBLE)      AS proteins_100g,
    CAST(list_filter(nutriments, x -> x.name = 'fiber')[1]['100g'] AS DOUBLE)         AS fiber_100g
FROM {{ source('nutriscan', 'food') }}
WHERE code IS NOT NULL