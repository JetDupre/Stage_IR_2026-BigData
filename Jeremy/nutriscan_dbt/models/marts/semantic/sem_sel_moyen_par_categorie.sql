-- moyenne de salt_g par categorie

{{ config(materialized='table') }}

SELECT
    dc.category_label,
    ROUND(AVG(fn.salt_100g), 4) AS sel_moyen_g
FROM {{ ref('fact_nutrition') }} fn
JOIN {{ ref('dim_category') }} dc ON fn.category_sk = dc.category_sk
WHERE fn.salt_100g IS NOT NULL
GROUP BY dc.category_label