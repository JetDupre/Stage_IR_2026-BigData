{{ config(materialized='table') }}

WITH categories_distinctes AS (
    SELECT
        category_tag,
        MAX(category_label) AS category_label
    FROM {{ ref('stg_categories') }}
    WHERE category_tag IS NOT NULL
    GROUP BY category_tag
)

SELECT
    ROW_NUMBER() OVER (ORDER BY category_tag) AS category_sk,
    category_tag,
    category_label
FROM categories_distinctes