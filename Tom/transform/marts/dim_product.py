# transform/marts/dim_product.py
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from transform.utils.spark_session import get_spark

spark = get_spark('dim_product')

# Lire depuis silver/
df = spark.read.parquet('s3a://silver/stg_products/')

# Deduplication et surrogate key
w = Window.orderBy('product_code')
df_dim = df.select(
    'product_code', 'product_name', 'nutriscore_grade',
    'nova_group', 'environmental_score_grade', 'quantity'
).dropDuplicates(['product_code']) \
 .withColumn('product_sk', row_number().over(w))

# Ecriture dans gold/
df_dim.write.mode('overwrite').parquet('s3a://gold/dim_product/')

# Creer la table Trino correspondante
# spark.sql('''
#     CREATE TABLE IF NOT EXISTS hive.nutriscan_gold.dim_product (
#         product_sk       INTEGER,
#         product_code     VARCHAR,
#         product_name     VARCHAR,
#         nutriscore_grade VARCHAR,
#         nova_group       INTEGER,
#         environmental_score_grade VARCHAR,
#         quantity VARCHAR
#     )
#     WITH (external_location = 's3a://gold/dim_product/', format = 'PARQUET')
# ''')

print(f'dim_product : {df_dim.count()} lignes')
spark.stop()
