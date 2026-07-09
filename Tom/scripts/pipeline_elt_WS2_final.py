from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
from pymongo import MongoClient
import time

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

# Upload du fichier Parquet brut dans raw/
s3.upload_file(
    Filename=SOURCE_PATH,
    Bucket='raw',
    Key='food.parquet',
    ExtraArgs={'ContentType': 'application/octet-stream'}
)

print(f'Upload MinIO terminé en {time.time()-start:.1f}s')

# Vérifier que le fichier est bien dans MinIO
response = s3.head_object(Bucket='raw', Key='food.parquet')
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
    'energy_kcal', 'fat_g', 'saturated_fat_g',     # On ajoute les colonnes nutritionnelles pour MongoDB
    'sugars_g', 'salt_g', 'fiber_g', 'proteins_g'
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

print(f'Temps total du pipeline ELT : {time.time()-temps1:.1f}s')