-- pourcentage de produits avec nutriscore_grade = 'a'

{{ config(materialized='table') }}

SELECT
    ROUND(100.0 * COUNT(CASE WHEN dp.nutriscore_grade = 'a' THEN 1 END) / COUNT(dp.nutriscore_grade), 2) AS pct_produits_nutriscore_a
FROM {{ ref('fact_nutrition') }} fn
JOIN {{ ref('dim_product') }} dp ON fn.product_sk = dp.product_sk