# transform/tests/test_dim_product.py
from transform.utils.spark_session import get_spark

spark = get_spark('test_dim_product')
df = spark.read.parquet('s3a://gold/dim_product/')
spark.sparkContext.setLogLevel('OFF')
errors = []

# Test 1 : product_sk non NULL
null_sk = df.filter(df.product_sk.isNull()).count()
if null_sk > 0:
    errors.append(f'ECHEC not_null product_sk : {null_sk} NULL')

# Test 2 : product_sk unique
total = df.count()
distinct = df.select('product_sk').distinct().count()
if total != distinct:
    errors.append(f'ECHEC unique product_sk : {total - distinct} doublons')

# Test 3 : product_code non NULL
null_code = df.filter(df.product_code.isNull()).count()
if null_code > 0:
    errors.append(f'ECHEC not_null product_code : {null_code} NULL')

# Test 4 : nutriscore_grade valeurs acceptees
invalid = df.filter(df.nutriscore_grade.isNotNull() &
    ~df.nutriscore_grade.isin('a','b','c','d','e')).count()

if invalid > 0:
    errors.append(f'ECHEC accepted_values nutriscore_grade : {invalid} invalides')

if errors:
    for e in errors: print(e)
else:
    print('Tous les tests passent pour dim_product')
spark.stop()
