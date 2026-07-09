# transform/staging/stg_products.py
from pyspark.sql.functions import col, when, trim
from transform.utils.spark_session import get_spark

spark = get_spark('stg_products')
spark.sparkContext.setLogLevel('ERROR')

# Lecture depuis MinIO raw/
df = spark.read.parquet('s3a://raw/food/food_20260622.parquet')

# Nettoyage et standardisation
df_clean = df.select(
    col('code').alias('product_code'),
    col('product_name').getItem(0).getField('text').alias('product_name'),    
    when(
        col('nutriscore_grade').isin('a','b','c','d','e'),
        col('nutriscore_grade')
    ).otherwise(None).alias('nutriscore_grade'),
    col('nova_group'),
    col('environmental_score_grade'),
    col('quantity'),
).filter(col('code').isNotNull())

# Ecriture dans MinIO silver/
df_clean.write \
    .mode('overwrite') \
    .parquet('s3a://silver/stg_products/')

print(f'stg_products : {df_clean.count()} lignes ecrites dans silver/')
spark.stop()
