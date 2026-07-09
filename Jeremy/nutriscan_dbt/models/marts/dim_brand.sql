{{ config(materialized='table') }}

WITH marques_distinctes AS (
    SELECT
        brand_tag,
        MAX(brand_name) AS brand_name
    FROM {{ ref('stg_brands') }}
    WHERE brand_tag IS NOT NULL
    GROUP BY brand_tag
)

SELECT
    ROW_NUMBER() OVER (ORDER BY brand_tag) AS brand_sk,
    brand_tag,
    brand_name
FROM marques_distinctes