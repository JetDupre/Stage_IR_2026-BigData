from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
from pymongo import MongoClient
import time
from datetime import datetime
import trino

temps1 = time.time()

# ─── 1. CONFIGURATION ────────────────────────────────────────────
MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS   = 'minioadmin'
MINIO_SECRET   = 'minioadmin'
MONGO_URI      = 'mongodb://admin:admin@localhost:27017'
SOURCE_PATH    = 'C:/data/nutriScan-docker/sources/food.parquet'

# ─── 2. SPARKSESSION ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('NutriScan-ELT') \
    .master('local[*]') \
    .config('spark.driver.memory', '8g') \
    .config('spark.sql.shuffle.partitions', '8') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')
print('SparkSession créée avec succès')

# ─── 3. EXTRACT ──────────────────────────────────────────────────
print('Lecture de food.parquet...')
start = time.time()

df = spark.read.parquet(SOURCE_PATH)

print(f'Lignes lues : {df.count():,}')
print(f'Colonnes : {len(df.columns)}')
print(f'Temps lecture : {time.time()-start:.1f}s')

# Afficher le schéma pour vérification
df.printSchema()

# ─── 4. LOAD VERS MINIO raw/ ─────────────────────────────────────
print('Chargement vers MinIO raw/...')
start = time.time()

# Connexion au client S3 MinIO
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET
)

# Note : Verifiez la connexion MinIO avant l'upload : 
try:
    s3.list_buckets()
    print('MinIO OK')
except Exception as e:
    print(f'ERREUR : {e}')
    raise

# Définir le nom food.parquet a la date du jour

date_str = datetime.now().strftime("%Y%m%d")
raw_object_name = f"food/food_{date_str}.parquet"

# Upload du fichier Parquet brut dans raw/
s3.upload_file(
    Filename=SOURCE_PATH,
    Bucket='raw',
    Key=raw_object_name,
    ExtraArgs={'ContentType': 'application/octet-stream'}
)

print(f'Upload MinIO terminé en {time.time()-start:.1f}s')

# Vérifier que le fichier est bien dans MinIO
response = s3.head_object(Bucket='raw', Key=raw_object_name)
print(f'Taille dans MinIO : {response["ContentLength"]/1e6:.0f} Mo')

# ─── 5. LOAD VERS MONGODB ────────────────────────────────────────
print('Chargement vers MongoDB...')
start = time.time()

# Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client['nutriscan']
collection = db['products_raw']

# Vider la collection si elle existe (idempotence)
collection.drop()
print('Collection products_raw réinitialisée')


# Fonction pour extraire une valeur nutritionnelle depuis l'array nutriments
# Exemple :
# name = 'fat'           -> récupère la valeur du champ `100g`
# output_col = 'fat_g'   -> nom de la colonne créée dans le DataFrame
def get_nutriment_100g(nutriment_name, output_col):
    return F.expr(
        f"element_at(transform(filter(nutriments, x -> x.name = '{nutriment_name}'), x -> x.`100g`), 1)"
    ).alias(output_col)


# Sélectionner un sous-ensemble pertinent pour MongoDB
# On ne charge pas 4,5M lignes - trop lourd pour MongoDB sur machine locale
# On charge les produits avec nutriscore connu (plus utiles pour les APIs)
df_mongo = df.filter(
    F.col('nutriscore_grade').isNotNull()
).select(
    'code', 'product_name', 'brands',
    'nutriscore_grade', 'nutriscore_score',
    'nova_group', 'environmental_score_grade',
    'categories_tags', 'countries_tags',
    # Colonnes nutritionnelles extraites depuis nutriments.name + nutriments.`100g`
    get_nutriment_100g('energy-kcal', 'energy_kcal'),
    get_nutriment_100g('fat', 'fat_g'),
    get_nutriment_100g('saturated-fat', 'saturated_fat_g'),
    get_nutriment_100g('sugars', 'sugars_g'),
    get_nutriment_100g('salt', 'salt_g'),
    get_nutriment_100g('fiber', 'fiber_g'),
    get_nutriment_100g('proteins', 'proteins_g')
    
).filter(F.col('energy_kcal') <= 900 #Filtre les valeurs aberrantes
).limit(100000)

print(f'Produits sélectionnés pour MongoDB : {df_mongo.count():,}')

# Convertir le DataFrame Spark en liste de dictionnaires Python
# pour l'insertion MongoDB
records = df_mongo.toPandas().to_dict('records')

# Insertion par lots de 1000 (batch_size)
# insert_many en une seule fois planterait sur 100 000 documents
batch_size = 1000
total_inserted = 0

for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    collection.insert_many(batch)
    total_inserted += len(batch)
    if total_inserted % 10000 == 0:
        print(f'  Inséré : {total_inserted:,} / {len(records):,}')

print(f'MongoDB : {total_inserted:,} documents insérés en {time.time()-start:.1f}s')
client.close()

# ─── 6. LOG D'EXECUTION PIPELINE DANS TRINO ─────────────────────
print('Écriture du log pipeline dans Trino...')

raw_output_file = f's3a://raw/{raw_object_name}'
nb_lignes_output = df.count()

run_id = f'PL_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
run_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

conn = trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='admin',
    catalog='hive',
    schema='nutriscan'
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_logs (
        run_id VARCHAR,
        run_date TIMESTAMP,
        source_file VARCHAR,
        output_file VARCHAR,
        nb_lignes_output INTEGER,
        statut VARCHAR
    )
    WITH (
        external_location = 's3a://raw/pipeline_logs/',
        format = 'PARQUET'
    )
""")

cur.execute(f"""
    INSERT INTO pipeline_logs VALUES (
        '{run_id}',
        TIMESTAMP '{run_date}',
        'food.parquet',
        '{raw_output_file}',
        {nb_lignes_output},
        'SUCCESS'
    )
""")

cur.close()
conn.close()

print(f'Log pipeline ajouté dans Trino : {run_id}')

print(f'Temps total du pipeline ELT : {time.time()-temps1:.1f}s')
