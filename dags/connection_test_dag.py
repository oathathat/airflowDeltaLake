from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession

def test_spark_connection():
    spark = SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()
    # If this runs without errors, the connection is successful.
    print(spark.version)

dag = DAG('test_spark_connection', 
          default_args={"owner": "Astro", "retries": 3}, 
          schedule_interval=None)

test_task = PythonOperator(
    task_id='test_spark_connection_task',
    python_callable=test_spark_connection,
    dag=dag,
)
