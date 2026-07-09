# transform/marts/dim_country.py
from pyspark.sql.functions import col, row_number, regexp_replace, initcap
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('dim_country')
spark.sparkContext.setLogLevel('ERROR')

# Lire depuis silver/
df = spark.read.parquet('s3a://silver/stg_countries/')

# Deduplication et surrogate key
w = Window.orderBy('country_tag')

df_dim = df.select(
    col('country_tag')
).filter(
    col('country_tag').isNotNull()
).filter(
    col('country_tag') != ''
).dropDuplicates(['country_tag']) \
 .withColumn(
    'country_name',
    initcap(regexp_replace(regexp_replace(col('country_tag'), '^..:', ''), '-', ' '))
).withColumn(
    'country_sk',
    row_number().over(w)
)

df_dim = df_dim.select(
    'country_sk',
    'country_tag',
    'country_name'
)

# Ecriture dans gold/
df_dim.write.mode('overwrite').parquet('s3a://gold/dim_country/')

print(f'dim_country : {df_dim.count()} lignes')
spark.stop()