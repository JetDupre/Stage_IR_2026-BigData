# transform/staging/stg_nutrition.py
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('stg_nutrition')
spark.sparkContext.setLogLevel('ERROR')
df = spark.read.parquet('s3a://raw/food/food_20260622.parquet')

# En PySpark, utiliser spark.sql() avec la syntaxe SQL Trino.
df.createOrReplaceTempView('food_raw')

# ANCIENNE REQHETE SQL
# df_nutrition = spark.sql("""
#     SELECT
#         code AS product_code,
#         IF(cardinality(filter(nutriments, n -> n.name = 'energy-kcal')) > 0,
#            filter(nutriments, n -> n.name = 'energy-kcal')[1].`100g`, NULL) AS energy_kcal,
#         IF(cardinality(filter(nutriments, n -> n.name = 'fat')) > 0,
#            filter(nutriments, n -> n.name = 'fat')[1].`100g`, NULL) AS fat_g,
#         IF(cardinality(filter(nutriments, n -> n.name = 'saturated-fat')) > 0,
#            filter(nutriments, n -> n.name = 'saturated-fat')[1].`100g`, NULL) AS saturated_fat_g,
#         IF(cardinality(filter(nutriments, n -> n.name = 'sugars')) > 0,
#            filter(nutriments, n -> n.name = 'sugars')[1].`100g`, NULL) AS sugars_g,
#         IF(cardinality(filter(nutriments, n -> n.name = 'fiber')) > 0,
#            filter(nutriments, n -> n.name = 'fiber')[1].`100g`, NULL) AS fiber_g,
#         IF(cardinality(filter(nutriments, n -> n.name = 'proteins')) > 0,
#            filter(nutriments, n -> n.name = 'proteins')[1].`100g`, NULL) AS proteins_g,
#         IF(cardinality(filter(nutriments, n -> n.name = 'salt')) > 0,
#            filter(nutriments, n -> n.name = 'salt')[1].`100g`, NULL) AS salt_g,
#         nutriscore_score

#     FROM food_raw WHERE code IS NOT NULL
# """)

df_nutrition = spark.sql("""
    SELECT
        code AS product_code,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'energy-kcal'),
                n -> n.`100g`
            ),
            1
        ) AS energy_kcal,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'fat'),
                n -> n.`100g`
            ),
            1
        ) AS fat_g,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'saturated-fat'),
                n -> n.`100g`
            ),
            1
        ) AS saturated_fat_g,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'sugars'),
                n -> n.`100g`
            ),
            1
        ) AS sugars_g,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'fiber'),
                n -> n.`100g`
            ),
            1
        ) AS fiber_g,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'proteins'),
                n -> n.`100g`
            ),
            1
        ) AS proteins_g,

        element_at(
            transform(
                filter(nutriments, n -> n.name = 'salt'),
                n -> n.`100g`
            ),
            1
        ) AS salt_g,

        nutriscore_score,
        to_date(from_unixtime(last_modified_t)) AS last_modified_date,
        last_modified_t

    FROM food_raw
    WHERE code IS NOT NULL
""")

# Garder uniquement la version la plus récente par produit
w = Window.partitionBy('product_code').orderBy(
    col('last_modified_t').desc_nulls_last()
)

df_nutrition = df_nutrition.withColumn(
    'rn',
    row_number().over(w)
).filter(
    col('rn') == 1
).drop(
    'rn',
    'last_modified_t'
)

df_nutrition.write.mode('overwrite').parquet('s3a://silver/stg_nutrition/')
print(f'stg_nutrition : {df_nutrition.count()} lignes')
spark.stop()
