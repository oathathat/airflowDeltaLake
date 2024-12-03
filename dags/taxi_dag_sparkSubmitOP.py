from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'taxi_bronze_spark_submit_operator',
    default_args={"owner": "oat", "retries": 3},
    description='ETL from source to Bronze Layer for Yellow Taxi data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    check_path = BashOperator(
        task_id='check_path',
        bash_command = 'pwd',      
        # cwd = '/usr/local/airflow' # default = none airflow will create temp directory to run script
    ) 
    # Spark submit task for Bronze layer ETL
    bronze_etl = SparkSubmitOperator(
        task_id='bronze_layer_etl',
        application='./dags/taxi-spark-job/bronze_layer_sparkSubmitOP.py',  
        name='BronzeLayer',
        conn_id='spark_default',  # Spark connection set up in AirflowUI (Admin->Connections)
        env_vars={'PATH': '/bin:/usr/bin:/usr/local/bin'}, #export path working directory        
        conf = {
            "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": S3_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": S3_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.connect.extensions.relation.classes":"org.apache.spark.sql.connect.delta.DeltaRelationPlugin",
            "spark.connect.extensions.command.classes":"org.apache.spark.sql.connect.delta.DeltaCommandPlugin",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.parquet.compression.codec":"gzip"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0",
    )
    check_path >> bronze_etl
