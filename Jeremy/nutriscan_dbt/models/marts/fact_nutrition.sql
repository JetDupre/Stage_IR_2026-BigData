{{ config(materialized='table') }}

WITH base_countries AS (
    SELECT * FROM {{ ref('stg_countries') }}
),

-- 1. On force l'unicité par product_code sur les métriques nutritionnelles
nutrition_unique AS (
    SELECT
        product_code,
        MAX(energy_kcal) AS energy_kcal,
        MAX(fat_100g) AS fat_100g,
        MAX(saturated_fat_100g) AS saturated_fat_100g,
        MAX(sugar_100g) AS sugar_100g,
        MAX(salt_100g) AS salt_100g,
        MAX(proteins_100g) AS proteins_100g,
        MAX(fiber_100g) AS fiber_100g,
        MAX(nutriscore_score) AS nutriscore_score,
        MAX(last_modified_t) AS last_modified_t
    FROM {{ ref('stg_nutrition') }}
    GROUP BY product_code
),

-- 2. On force l'unicité par product_code sur le pont des catégories
cats_unique AS (
    SELECT
        product_code,
        MAX(category_tag) AS category_tag
    FROM {{ ref('stg_categories') }}
    GROUP BY product_code
),

-- 3. On force l'unicité par product_code sur le pont des marques
brands_unique AS (
    SELECT
        product_code,
        MAX(brand_tag) AS brand_tag
    FROM {{ ref('stg_brands') }}
    GROUP BY product_code
),

-- 4. On force l'unicité par product_code sur le pont des dates
dates_unique AS (
    SELECT
        product_code,
        MAX(date_sk) AS date_sk
    FROM {{ ref('stg_date') }}
    GROUP BY product_code
),

-- Import des dimensions physiques dédoublonnées (Gold)
dim_p  AS (SELECT * FROM {{ ref('dim_product') }}),
dim_c  AS (SELECT * FROM {{ ref('dim_category') }}),
dim_b  AS (SELECT * FROM {{ ref('dim_brand') }}),
dim_co AS (SELECT * FROM {{ ref('dim_country') }}),
dim_d  AS (SELECT * FROM {{ ref('dim_date') }})

SELECT
    -- Clé primaire de fait
    ROW_NUMBER() OVER (ORDER BY c.product_code, c.country_tag) AS nutrition_sk,
    
    -- Clés étrangères de substitution
    p.product_sk,
    dc.category_sk,
    db.brand_sk,
    dco.country_sk,
    d.date_sk,
    
    -- Métriques nutritionnelles
    n.energy_kcal,
    n.fat_100g,
    n.saturated_fat_100g,
    n.sugar_100g,
    n.salt_100g,
    n.proteins_100g,
    n.fiber_100g,
    n.nutriscore_score

FROM base_countries c

-- Jointure directe sur la nutrition nettoyée (1-à-1 strict)
LEFT JOIN nutrition_unique n ON c.product_code = n.product_code

-- Jointure Produit (1-à-1)
LEFT JOIN dim_p p            ON c.product_code = p.product_code

-- Jointure Pays (1-à-1 strict puisque dim_country est dédoublonnée)
LEFT JOIN dim_co dco         ON c.country_tag  = dco.country_tag

-- Jointure Catégorie sécurisée via le pont dédoublonné
LEFT JOIN cats_unique cu     ON c.product_code = cu.product_code
LEFT JOIN dim_c dc           ON cu.category_tag = dc.category_tag

-- Jointure Marque sécurisée via le pont dédoublonné
LEFT JOIN brands_unique bu   ON c.product_code = bu.product_code
LEFT JOIN dim_b db           ON bu.brand_tag   = db.brand_tag

-- Jointure Temporelle sécurisée via le pont dédoublonné
LEFT JOIN dates_unique du    ON c.product_code = du.product_code
LEFT JOIN dim_d d            ON du.date_sk     = d.date_sk