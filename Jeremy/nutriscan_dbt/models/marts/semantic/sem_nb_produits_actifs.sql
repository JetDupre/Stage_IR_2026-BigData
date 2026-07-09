-- COUNT DISTINCT product_sk total

{{ config(materialized='table') }}

SELECT
    COUNT(DISTINCT fn.product_sk) AS nb_produits_actifs
FROM {{ ref('fact_nutrition') }} fn