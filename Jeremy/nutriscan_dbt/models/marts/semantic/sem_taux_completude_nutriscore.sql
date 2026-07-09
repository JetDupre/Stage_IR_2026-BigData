-- pourcentage de produits avec nutriscore_score non NULL

{{ config(materialized='table') }}

SELECT
    ROUND(100.0 * COUNT(fn.nutriscore_score) / COUNT(*), 2) AS taux_completude_nutriscore
FROM {{ ref('fact_nutrition') }} fn