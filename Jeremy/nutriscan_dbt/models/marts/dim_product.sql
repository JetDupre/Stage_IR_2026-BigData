{{ config(materialized='table') }}

WITH produits_uniques AS (
    SELECT
        product_code,
        FIRST(product_name) AS product_name, -- Sécurise 1 seul nom par code EAN
        FIRST(nutriscore_grade) AS nutriscore_grade,
        FIRST(nova_group) AS nova_group,
        FIRST(environmental_score_grade) AS environmental_score_grade,
        FIRST(product_quantity) AS product_quantity
    FROM {{ ref('stg_products') }}
    WHERE product_code IS NOT NULL
    GROUP BY product_code -- GARANTIT l'unicité stricte de la clé naturelle !
)
SELECT
    ROW_NUMBER() OVER (ORDER BY product_code) AS product_sk,
    product_code,
    product_name,
    nutriscore_grade,
    nova_group,
    environmental_score_grade,
    product_quantity
FROM produits_uniques