{{ config(materialized='view') }}

SELECT DISTINCT
    code                                    AS product_code,
    
    -- Extraction du nom commercial principal ('main'), avec un repli (coalesce) sur le premier élément si 'main' est absent
    COALESCE(
        list_filter(product_name, x -> x.lang = 'main')[1]['text'],
        product_name[1]['text']
    )                                       AS product_name,
    
    CASE
        WHEN nutriscore_grade IN ('a','b','c','d','e')
        THEN nutriscore_grade
        ELSE NULL
    END                                     AS nutriscore_grade,
    nova_group,
    environmental_score_grade,
    quantity                                AS product_quantity
FROM {{ source('nutriscan', 'food') }}
WHERE code IS NOT NULL