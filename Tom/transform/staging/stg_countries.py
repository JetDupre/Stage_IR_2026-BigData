# transform/staging/stg_countries.py
from pyspark.sql.functions import col, explode, split, trim, lower
from transform.utils.spark_session import get_spark

spark = get_spark('stg_countries')
spark.sparkContext.setLogLevel('ERROR')

# Lecture depuis MinIO raw/
df = spark.read.parquet('s3a://raw/food/food_20260622.parquet')

df_countries = df.select(
    col('code').alias('product_code'),
    explode(col('countries_tags')).alias('country_tag')
).withColumn('country_tag', lower(trim(col('country_tag'))))\
    .filter(col('product_code').isNotNull()) \
    .filter((col('country_tag').isNotNull()) & (col('country_tag') != '')) # Exclure les country_tag vides ou NULL

df_countries = df_countries.dropDuplicates(['product_code', 'country_tag'])

df_countries.write.mode('overwrite').parquet('s3a://silver/stg_countries/')
print(f'stg_countries : {df_countries.count()} lignes')
spark.stop()
