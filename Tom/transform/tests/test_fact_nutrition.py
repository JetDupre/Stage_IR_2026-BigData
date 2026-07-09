# transform/tests/test_fact_nutrition.py
from transform.utils.spark_session import get_spark

spark = get_spark('test_fact_nutrition')
spark.sparkContext.setLogLevel('OFF')

df = spark.read.parquet('s3a://gold/fact_nutrition/')

errors = []
success = []

# Test 1 : product_sk non NULL
null_product_sk = df.filter(df.product_sk.isNull()).count()
if null_product_sk > 0:
    errors.append(f'ECHEC not_null product_sk : {null_product_sk} NULL')
else:
    success.append('OK not_null product_sk')

# Test 2 : category_sk non NULL
null_category_sk = df.filter(df.category_sk.isNull()).count()
if null_category_sk > 0:
    errors.append(f'ECHEC not_null category_sk : {null_category_sk} NULL')
else:
    success.append('OK not_null category_sk')

# Test 3 : brand_sk non NULL
null_brand_sk = df.filter(df.brand_sk.isNull()).count()
if null_brand_sk > 0:
    errors.append(f'ECHEC not_null brand_sk : {null_brand_sk} NULL')
else:
    success.append('OK not_null brand_sk')

# Test 4 : country_sk non NULL
null_country_sk = df.filter(df.country_sk.isNull()).count()
if null_country_sk > 0:
    errors.append(f'ECHEC not_null country_sk : {null_country_sk} NULL')
else:
    success.append('OK not_null country_sk')

# Test 5 : date_sk non NULL
null_date_sk = df.filter(df.date_sk.isNull()).count()
if null_date_sk > 0:
    errors.append(f'ECHEC not_null date_sk : {null_date_sk} NULL')
else:
    success.append('OK not_null date_sk')

# Test 6 : energy_kcal positive ou nulle
invalid_energy = df.filter(
    df.energy_kcal.isNotNull() & (df.energy_kcal < 0)
).count()

if invalid_energy > 0:
    errors.append(f'ECHEC positive_values energy_kcal : {invalid_energy} valeurs negatives')
else:
    success.append('OK positive_values energy_kcal')

# Test 7 : salt_g positif ou nul
invalid_salt = df.filter(
    df.salt_g.isNotNull() & (df.salt_g < 0)
).count()

if invalid_salt > 0:
    errors.append(f'ECHEC positive_values salt_g : {invalid_salt} valeurs negatives')
else:
    success.append('OK positive_values salt_g')

# Test 8 : nutriscore_score entre -15 et 40
invalid_nutriscore_score = df.filter(
    df.nutriscore_score.isNotNull() &
    (
        (df.nutriscore_score < -15) |
        (df.nutriscore_score > 40)
    )
).count()

if invalid_nutriscore_score > 0:
    errors.append(
        f'ECHEC accepted_range nutriscore_score [-15, 40] : {invalid_nutriscore_score} invalides'
    )
else:
    success.append('OK accepted_range nutriscore_score [-15, 40]')

# Résultat des tests
print('--- TESTS REUSSIS ---')
for s in success:
    print(s)

print('--- TESTS EN ECHEC ---')
if errors:
    for e in errors:
        print(e)
else:
    print('Aucun test en echec')

spark.stop()