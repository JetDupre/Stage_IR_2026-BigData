# transform/staging/stg_brands.py
from pyspark.sql.functions import col, split, trim, upper, lower, max as F_max
from transform.utils.spark_session import get_spark

spark = get_spark('stg_brands')
spark.sparkContext.setLogLevel('FATAL')

# Lecture depuis MinIO raw/
df = spark.read.parquet('s3a://raw/food/food_20260622.parquet')

# Nettoyage et standardisation
df_clean = df.select(
    col('code').alias('product_code'),
    upper(trim(split(col('brands'), ',').getItem(0))).alias('brand_name'),
    lower(trim(col('brands_tags').getItem(0))).alias('brand_tag')
).filter(
    col('product_code').isNotNull()
).filter(
    (col('brand_name').isNotNull()) & (col('brand_name') != '')
)

# 1 seule marque par produit
df_clean = df_clean.groupBy('product_code').agg(
    F_max('brand_name').alias('brand_name'),
    F_max('brand_tag').alias('brand_tag')
)

# Ecriture dans MinIO silver/
df_clean.write \
    .mode('overwrite') \
    .parquet('s3a://silver/stg_brands/')

print(f'stg_brands : {df_clean.count()} lignes ecrites dans silver/')
spark.stop()
