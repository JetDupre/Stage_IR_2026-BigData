{{ config(materialized='view') }}

WITH base_date AS (
    SELECT
        code AS product_code,
        CAST(to_timestamp(last_modified_t) AS DATE) AS date_day
    FROM {{ source('nutriscan', 'food') }}
    WHERE last_modified_t IS NOT NULL
)
SELECT
    product_code,
    date_day AS date_value,
    CAST(strftime(date_day, '%Y%m%d') AS INTEGER) AS date_sk, -- Format intelligent AAAAMMDD
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(quarter FROM date_day) AS quarter,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    EXTRACT(isodow FROM date_day)                                AS day_of_week,
    CAST(FLOOR(EXTRACT(year FROM date_day) / 10) * 10 AS INTEGER) AS decade,
    CASE EXTRACT(month FROM date_day)
        WHEN 1 THEN 'Janvier' WHEN 2 THEN 'Février' WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avril'   WHEN 5 THEN 'Mai'      WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet' WHEN 8 THEN 'Août'     WHEN 9 THEN 'Septembre'
        WHEN 10 THEN 'Octobre' WHEN 11 THEN 'Novembre' WHEN 12 THEN 'Décembre'
    END AS month_name,
    CASE 
        WHEN EXTRACT(isodow FROM date_day) IN (6, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend -- Identification du samedi (6) et dimanche (7)
FROM base_date