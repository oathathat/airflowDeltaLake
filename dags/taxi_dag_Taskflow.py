"""
DAG with Taskflow API coding style
create Global SparkSession may cause DAG import Error 
(run first time is about 2 minute waiting for spark download extra lib)
"""

from airflow.decorators import task, dag
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
S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")
minio_bucket_path_b = 's3a://taxi/bronze/yellow'
temp_path = f"{minio_bucket_path_b}/raw/yellow_df.parquet"

@dag(
    "taxi_bronze_taskflow",
    default_args=default_args,
    description='ETL from source to Bronze Layer for Yellow Taxi data with Task Flow API coding style',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['taxi', 'bronze']
)
def BronzeLayerETLTaskFlow():      
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

    @task(task_id="extract_from_source")
    def extract_from_source():
        data_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
        spark.sparkContext.addFile(data_path)
        yellow_df = spark.read.parquet(f'file://{SparkFiles.get("yellow_tripdata_2024-01.parquet")}')
        yellow_df.write.format("parquet").mode("overwrite").save(temp_path)
        return temp_path
    
    @task(task_id="add_meta_data_col")
    def add_meta_data_col(path: str):
        df = spark.read.parquet(path)
        df = df.withColumn('ingest_date', current_date()).withColumn('source', lit('yellow'))
        temp_path = f"{minio_bucket_path_b}/raw2/yellow_df.parquet"
        df.write.format("parquet").mode("overwrite").save(temp_path)
        return temp_path

    @task(task_id="load_to_bronze")
    def load_to_bronze(path: str):
        df = spark.read.parquet(path)
        yellow_b_path = f"{minio_bucket_path_b}/final"
        df.write.format("delta").mode('append').option('mergeSchema', 'true').partitionBy('ingest_date').save(yellow_b_path)
    
    @task(task_id="stop_bronze_session")
    def stop_bronze_session(depend_on):
        spark.stop()      

    # setup_spark = setup_session()
    temp_file_path = extract_from_source()
    temp_file_path = add_meta_data_col(temp_file_path)
    next_depend = load_to_bronze(temp_file_path) 
    stop_bronze_session(next_depend)    
    
BronzeLayerETLTaskFlow()