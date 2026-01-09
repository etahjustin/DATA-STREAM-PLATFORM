import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import *

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "validated-transactions"

# Configuration MinIO (S3)
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio_access_key"
SECRET_KEY = "minio_secret_key"
BUCKET_NAME = "datalake"

def create_spark_session():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "io.delta:delta-spark_2.12:3.0.0"
    ]
    
    conf = SparkSession.builder \
        .appName("LakeManager") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "-Dorg.apache.spark.serializer.KryoSerializer")

    return conf.getOrCreate()

def main():
    try:
        shutil.rmtree("./metastore_db", ignore_errors=True)
        shutil.rmtree("./derby.log", ignore_errors=True)
    except:
        pass

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(" Démarrage du Lake Manager (Spark Structured Streaming)")
    print(" Version Spark:", spark.version)

    # 1. Définition du Schéma
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("origin_ip", StringType(), True),
        StructField("error_reason", StringType(), True)
    ])

    try:
        raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
    except Exception as e:
        print(f" Erreur critique Kafka : {e}")
        return

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = parsed_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/transactions") \
        .option("path", f"s3a://{BUCKET_NAME}/tables/transactions") \
        .start()

    print(f" Streaming en cours vers s3a://{BUCKET_NAME}/tables/transactions ...")
    query.awaitTermination()

if __name__ == "__main__":
    main()