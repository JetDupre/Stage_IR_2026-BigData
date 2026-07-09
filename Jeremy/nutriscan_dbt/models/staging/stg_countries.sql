{{ config(materialized='view') }}

WITH unnested_countries AS (
    SELECT 
        code AS product_code,
        UNNEST(countries_tags) AS country_tag 
    FROM {{ source('nutriscan', 'food') }}
    WHERE code IS NOT NULL AND countries_tags IS NOT NULL
)
SELECT product_code, TRIM(country_tag) AS country_tag
FROM unnested_countries
WHERE country_tag IS NOT NULL AND country_tag != ''