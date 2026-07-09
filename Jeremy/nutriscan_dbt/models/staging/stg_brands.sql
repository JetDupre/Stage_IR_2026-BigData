{{ config(materialized='view') }}

WITH raw_brands AS (
    SELECT
        code AS product_code,
        LOWER(TRIM(brands_tags[1])) AS brand_tag,
        TRIM(string_split(brands, ',')[1]) AS raw_brand_name
    FROM {{ source('nutriscan', 'food') }}
    WHERE brands IS NOT NULL AND brands != '' AND code IS NOT NULL
)
SELECT
    product_code,
    brand_tag,
    -- Ta formule DuckDB pour forcer la première lettre en Majuscule et le reste en Minuscule
    UPPER(SUBSTR(raw_brand_name, 1, 1)) || LOWER(SUBSTR(raw_brand_name, 2)) AS brand_name
FROM raw_brands