{{ config(materialized='table') }}

WITH pays_distincts AS (
    SELECT DISTINCT country_tag
    FROM {{ ref('stg_countries') }}
    WHERE country_tag IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY country_tag) AS country_sk,
    country_tag,
    UPPER(SUBSTR(country_tag, 4, 1)) || SUBSTR(country_tag, 5) AS country_name
FROM pays_distincts