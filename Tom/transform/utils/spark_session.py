# transform/utils/spark_session.py
import os
if os.name == 'nt':
    os.environ['HADOOP_HOME'] = 'C:/hadoop'
    os.environ['PATH'] += ';C:/hadoop/bin'

from pyspark.sql import SparkSession
 
def get_spark(app_name='NutriScan'):
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    return SparkSession.builder \
        .appName(app_name) \
        .master('local[*]') \
        .config('spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:3.3.4,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.262') \
        .config('spark.sql.shuffle.partitions', '4') \
        .config('spark.hadoop.fs.s3a.endpoint', minio_endpoint) \
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
        .config('spark.driver.memory', '8g') \
        .getOrCreate()