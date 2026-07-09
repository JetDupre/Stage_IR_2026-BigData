# transform/staging/stg_category.py
from pyspark.sql.functions import col, explode, trim, regexp_replace, initcap, max as F_max
from transform.utils.spark_session import get_spark

spark = get_spark('stg_category')
spark.sparkContext.setLogLevel('ERROR')

# Lecture depuis MinIO raw/
df = spark.read.parquet('s3a://raw/food/food_20260622.parquet')

# Extraction des catégories
df_category = df.select(
    col('code').alias('product_code'),
    col('categories_tags').getItem(0).alias('category_tag')
).withColumn(
    'category_tag',
    trim(col('category_tag'))
).filter(
    col('product_code').isNotNull()
).filter(
    (col('category_tag').isNotNull()) & (col('category_tag') != '')
)

# Création d'un category_name lisible depuis le tag
# Exemple : en:sweet-spreads -> Sweet Spreads
df_category = df_category.withColumn(
    'category_name',
    initcap(
        regexp_replace(
            regexp_replace(col('category_tag'), '^..:', ''),
            '-',
            ' '
        )
    )
)

# 1 seule catégorie par produit
df_category = df_category.groupBy('product_code').agg(
    F_max('category_tag').alias('category_tag'),
    F_max('category_name').alias('category_name')
)

# Ecriture dans MinIO silver/
df_category.write \
    .mode('overwrite') \
    .parquet('s3a://silver/stg_category/')

print(f'stg_category : {df_category.count()} lignes ecrites dans silver/')

spark.stop()