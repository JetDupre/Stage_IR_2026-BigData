# transform/marts/dim_brand.py
from pyspark.sql.functions import col, row_number, lower, regexp_replace, trim, first
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('dim_brand')
spark.sparkContext.setLogLevel('ERROR')

# Lire depuis silver/
df = spark.read.parquet('s3a://silver/stg_brands/')

df_dedup = df.filter(
    col('brand_name').isNotNull()
).filter(
    col('brand_name') != ''
).groupBy(
    'brand_name'
).agg(
    first('brand_tag', ignorenulls=True).alias('brand_tag')
)

# Deduplication et surrogate key
w = Window.orderBy('brand_name')

df_dim = df_dedup.withColumn('brand_sk', row_number().over(w))

df_dim = df_dim.select(
    'brand_sk',
    'brand_name',
    'brand_tag'
)

# Ecriture dans gold/
df_dim.write.mode('overwrite').parquet('s3a://gold/dim_brand/')

print(f'dim_brand : {df_dim.count()} lignes')
spark.stop()