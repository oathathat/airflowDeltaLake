from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from pyspark import SparkFiles
import os

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Environment variables for MinIO
S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")
minio_bucket_path_b = 's3a://taxi/bronze/yellow'

def etl_process():
    """Complete ETL process: Start Spark session, read, transform, and load data."""
    # Start Spark session
    spark = (
        SparkSession.builder
        .appName("BronzeLayer")
        # .master("spark://spark-master:7077")
        .master("local[*]")
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
    try:
        # Read data from source
        data_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
        spark.sparkContext.addFile(data_path)
        # print(SparkFiles.get("yellow_tripdata_2024-01.parquet"))
        yellow_df = spark.read.parquet(f'file://{SparkFiles.get("yellow_tripdata_2024-01.parquet")}')

        # Transform the data
        yellow_df = yellow_df.withColumn('ingest_date', current_date()).withColumn('source', lit('yellow'))

        # Load the data to the bronze layer
        yellow_b_path = f"{minio_bucket_path_b}/yellow"
        yellow_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('ingest_date').save(yellow_b_path)

    finally:
        # Stop the Spark session
        spark.stop()


    
# DAG Definition
with DAG(
    'taxi_bronze_layer_etl_python_operator',
    default_args=default_args,
    description='ETL from source to Bronze Layer for Yellow Taxi data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Single task to run the entire ETL process
    etl_task = PythonOperator(
        task_id='etl_task_bronze_layer',
        python_callable=etl_process,
    )
    etl_task
