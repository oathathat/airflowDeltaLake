from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.baseoperator import chain

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'taxi_bronze_layer_etl_bash_operator',
    default_args=default_args,
    description='ETL from source to Bronze Layer for Yellow Taxi data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:  
    #Debug path error
    check_path = BashOperator(
        task_id='check_path',
        bash_command = 'pwd',      
        cwd = '/usr/local/airflow' # default = none airflow will create temp directory to run script
    )  
    bronze_task_y = BashOperator(
        task_id='etl_bronze_taxi_yellow',
        bash_command = 'python dags/taxi-spark-job/yellow/bronze_layer_batchOP.py', #
        cwd = '/usr/local/airflow'
    )
    silver_task_y = BashOperator(
        task_id='etl_silver_taxi_yellow',
        bash_command = 'python dags/taxi-spark-job/yellow/silver_layer_batchOP.py', #
        cwd = '/usr/local/airflow'
    )
    bronze_task_g = BashOperator(
        task_id= 'etl_bronze_taxi_green',
        bash_command = 'python dags/taxi-spark-job/green/b_green_batchOP.py', 
        cwd = '/usr/local/airflow'
    )
    silver_task_g = BashOperator(
        task_id= 'etl_silver_taxi_green',
        bash_command = 'python dags/taxi-spark-job/green/s_green_batchOP.py', 
        cwd = '/usr/local/airflow'
    )
    
    gold_task = BashOperator(
        task_id='etl_gold_taxi',
        bash_command = 'python dags/taxi-spark-job/gold_layer_batchOP.py', #
        cwd = '/usr/local/airflow'
    )
    dim_task = BashOperator(
        task_id='etl_dim_taxi',
        bash_command = 'python dags/taxi-spark-job/dim_table_batchOP.py', #
        cwd = '/usr/local/airflow'
    )
    
    chain(check_path,bronze_task_y,silver_task_y,gold_task)
    chain(check_path,bronze_task_g,silver_task_g,gold_task)
    dim_task