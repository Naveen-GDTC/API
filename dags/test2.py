from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def hello_world():
    print("Hello, World!")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 20),  
    'retries': 1,                         
}

with DAG(
    'hello_world_dag_test_2',                   
    default_args=default_args,
    schedule_interval='@daily',          
    catchup=False,                       
) as dag:

   
    hello_task = PythonOperator(
        task_id='print_hello_world',      
        python_callable=hello_world,      
    )

    hello_task
