# transform/marts/fact_nutrition.py
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('fact_nutrition')

# Lecture des sources
nutrition  = spark.read.parquet('s3a://silver/stg_nutrition/')
countries  = spark.read.parquet('s3a://silver/stg_countries/')
categories = spark.read.parquet('s3a://silver/stg_category/')
brands     = spark.read.parquet('s3a://silver/stg_brands/')
dates      = spark.read.parquet('s3a://silver/stg_date/')
dim_prod   = spark.read.parquet('s3a://gold/dim_product/')
dim_cat    = spark.read.parquet('s3a://gold/dim_category/')
dim_brand  = spark.read.parquet('s3a://gold/dim_brand/')
dim_country= spark.read.parquet('s3a://gold/dim_country/')
dim_date   = spark.read.parquet('s3a://gold/dim_date/')

# Construction de la table de faits
fact = nutrition \
    .join(countries,   'product_code', 'inner') \
    .join(dim_prod,    'product_code', 'left') \
    .join(categories,  'product_code', 'left') \
    .join(dim_cat,     'category_tag', 'left') \
    .join(brands,      'product_code', 'left') \
    .join(dim_brand,   'brand_name', 'left') \
    .join(dim_country, 'country_tag', 'left') \
    .join(dates,       'product_code', 'left') \
    .join(dim_date,    col('last_modified_date') == dim_date['full_date'], 'left') \
    .select( #AJOUT ICI
        dim_prod['product_sk'],
        dim_cat['category_sk'],
        dim_brand['brand_sk'],
        dim_country['country_sk'],
        dim_date['date_sk'],
        col('energy_kcal'),
        col('fat_g'),
        col('salt_g'),
        col('nutriscore_score'), #AJOUT ICI
        col('saturated_fat_g'),
        col('sugars_g'),
        col('fiber_g'),
        col('proteins_g')
    )

# Supprime les doublons sur les colonnes de la table de faits à cause de jointures multiples
fact = fact.dropDuplicates([
    'product_sk',
    'category_sk',
    'brand_sk',
    'country_sk',
    'date_sk',
    'energy_kcal',
    'fat_g',
    'salt_g',
    'nutriscore_score',
    'saturated_fat_g',
    'sugars_g',
    'fiber_g',
    'proteins_g'
])


# Ajout d'une clé technique pour la table de faits
w = Window.orderBy(
    'product_sk',
    'category_sk',
    'brand_sk',
    'country_sk',
    'date_sk'
)
fact = fact.withColumn(
    'nutrition_fact_sk',
    row_number().over(w)
)

# Remettre les colonnes dans l'ordre final
fact = fact.select(
    'nutrition_fact_sk',

    'product_sk',
    'category_sk',
    'brand_sk',
    'country_sk',
    'date_sk',

    'energy_kcal',
    'fat_g',
    'salt_g',
    'nutriscore_score',
    'saturated_fat_g',
    'sugars_g',
    'fiber_g',
    'proteins_g'
)

fact.write.mode('overwrite').parquet('s3a://gold/fact_nutrition/')
print(f'fact_nutrition : {fact.count()} lignes')
spark.stop()
