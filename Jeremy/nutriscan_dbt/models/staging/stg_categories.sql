{{ config(materialized='view') }}

WITH raw_categories AS (
    SELECT
        code AS product_code,
        LOWER(TRIM(categories_tags[1])) AS category_tag,
        TRIM(string_split(categories, ',')[1]) AS raw_category_label
    FROM {{ source('nutriscan', 'food') }}
    WHERE categories_tags IS NOT NULL AND code IS NOT NULL
)
SELECT
    product_code,
    category_tag,
    -- Normalisation uniforme de la casse du libellé de la catégorie
    UPPER(SUBSTR(raw_category_label, 1, 1)) || LOWER(SUBSTR(raw_category_label, 2)) AS category_label
FROM raw_categories