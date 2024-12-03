from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from pyspark import SparkFiles

import os


S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

# Initialize Spark session with configurations
spark = (
    SparkSession.builder
    .appName("BronzeLayer_green")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.connect.extensions.relation.classes", "org.apache.spark.sql.connect.delta.DeltaRelationPlugin")
        .config("spark.connect.extensions.command.classes", "org.apache.spark.sql.connect.delta.DeltaCommandPlugin")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()   
)

# Load source data
green_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet'
spark.sparkContext.addFile(green_path)
green_df = spark.read.parquet(f"file://{SparkFiles.get("green_tripdata_2024-01.parquet")}")

# Transform data
green_df = (
    green_df.withColumn('ingest_date',current_date())
            .withColumn('source',lit('green'))
)
# Write to bronze layer with Delta format
green_df.write.format('delta') \
    .mode('append') \
    .option('mergeSchema', 'true') \
    .partitionBy('ingest_date') \
    .save("s3a://taxi/bronze/green")
    
# Stop Spark session
spark.stop()
