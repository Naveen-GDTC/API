from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def hello_world():
    df = pd.DataFrame({"Name":['1','2','3'],
                       "Age":[1,2,3]})
    print(df)
    print("Hello, World!")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 20),  
    'retries': 1,                         
}

with DAG(
    'hello_world_dag',                   
    default_args=default_args,
    schedule_interval='@daily',          
    catchup=False,                       
) as dag:

   
    hello_task = PythonOperator(
        task_id='print_hello_world',      
        python_callable=hello_world,      
    )

    hello_task
