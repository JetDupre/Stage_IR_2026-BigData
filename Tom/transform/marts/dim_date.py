# transform/marts/dim_date.py
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('dim_date')
spark.sparkContext.setLogLevel('ERROR')

# Lire depuis silver/
df = spark.read.parquet('s3a://silver/stg_date/')

# Deduplication et surrogate key
w = Window.orderBy('full_date')

df_dim = df.select(
    'full_date',
    'year',
    'quarter',
    'month',
    'day_of_week',
    'is_weekend'
).filter(
    col('full_date').isNotNull()
).dropDuplicates(['full_date']) \
 .withColumn(
    'date_sk',
    row_number().over(w)
)

df_dim = df_dim.select(
    'date_sk',
    'full_date',
    'year',
    'quarter',
    'month',
    'day_of_week',
    'is_weekend'
)

# Ecriture dans gold/
df_dim.write.mode('overwrite').parquet('s3a://gold/dim_date/')

print(f'dim_date : {df_dim.count()} lignes')
spark.stop()