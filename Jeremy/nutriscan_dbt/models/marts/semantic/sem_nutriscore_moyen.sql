-- Elle calcule le nutriscore moyen par categorie, marque et pays en une seule requete.

{{ config(materialized='table') }}

SELECT
    dc.category_label,
    db.brand_name,
    dco.country_name,
    dd.year,
    dd.month,
    ROUND(AVG(fn.nutriscore_score), 2)  AS nutriscore_moyen,
    COUNT(DISTINCT fn.product_sk)        AS nb_produits
FROM {{ ref('fact_nutrition') }} fn
JOIN {{ ref('dim_category') }}  dc  ON fn.category_sk  = dc.category_sk
JOIN {{ ref('dim_brand') }}     db  ON fn.brand_sk     = db.brand_sk
JOIN {{ ref('dim_country') }}   dco ON fn.country_sk   = dco.country_sk
JOIN {{ ref('dim_date') }}      dd  ON fn.date_sk      = dd.date_sk
WHERE fn.nutriscore_score IS NOT NULL
GROUP BY dc.category_label, db.brand_name, dco.country_name, dd.year, dd.month