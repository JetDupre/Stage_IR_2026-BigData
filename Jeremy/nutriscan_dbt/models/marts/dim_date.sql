{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT
        date_sk,
        date_value,
        decade,
        year,
        quarter,
        month,
        day_of_week,
        is_weekend
    FROM {{ ref('stg_date') }}
    WHERE date_sk IS NOT NULL
)
SELECT * FROM dates