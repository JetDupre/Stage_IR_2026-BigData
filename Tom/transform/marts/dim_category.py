# transform/marts/dim_category.py
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('dim_category')
spark.sparkContext.setLogLevel('ERROR')

# Lire depuis silver/
df = spark.read.parquet('s3a://silver/stg_category/')

# Deduplication et surrogate key
w = Window.orderBy('category_tag')

df_dim = df.select(
    'category_tag',
    'category_name'
).filter(
    col('category_tag').isNotNull()
).filter(
    col('category_tag') != ''
).dropDuplicates(['category_tag']) \
 .withColumn('category_sk', row_number().over(w))

# Remettre les colonnes dans l'ordre attendu
df_dim = df_dim.select(
    'category_sk',
    'category_tag',
    'category_name'
)

# Ecriture dans gold/
df_dim.write.mode('overwrite').parquet('s3a://gold/dim_category/')

print(f'dim_category : {df_dim.count()} lignes')

spark.stop()