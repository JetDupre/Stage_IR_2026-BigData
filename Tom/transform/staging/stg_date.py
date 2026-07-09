# transform/staging/stg_date.py
from pyspark.sql.functions import col, trim, to_date, from_unixtime, year, quarter, month, dayofweek, when, max as F_max
from transform.utils.spark_session import get_spark

spark = get_spark('stg_date')
spark.sparkContext.setLogLevel('FATAL')

# Lecture depuis MinIO raw/
df = spark.read.parquet('s3a://raw/food/food_20260622.parquet')

# Conversion timestamp to date
df_clean = df.select(
    col('code').alias('product_code'),
    to_date(from_unixtime(col('last_modified_t'))).alias('full_date'))\
.filter(
    col('product_code').isNotNull())\
.filter(
    col('full_date').isNotNull())

# 1 seule date par produit : on garde la plus récente
df_clean = df_clean.groupBy('product_code').agg(
    F_max('full_date').alias('full_date')
)

df_clean =df_clean.select(
    col('product_code'),
    col('full_date'),
    year(col('full_date')).alias('year'),
    quarter(col('full_date')).alias('quarter'),
    month(col('full_date')).alias('month'),
    dayofweek(col('full_date')).alias('day_of_week'),

    when(
        dayofweek(col('full_date')).isin(1,7),True
    ).otherwise(False).alias('is_weekend')
)

# Ecriture dans MinIO silver/
df_clean.write \
    .mode('overwrite') \
    .parquet('s3a://silver/stg_date/')

print(f'stg_date : {df_clean.count()} lignes ecrites dans silver/')
spark.stop()