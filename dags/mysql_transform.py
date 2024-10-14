from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd
import requests
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


with DAG(
    dag_id='mysql_data_transformation',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
) as dag:
    def fetch_data_from_api():
        url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
        params = {
            'frequency': 'hourly',
            'data[0]': 'value',
            'sort[0][column]': 'period',  
            'sort[0][direction]': 'desc',
            'offset': 0,
            'length': 5000,
            'api_key': 'q5n29GJXkd5xd0nznZ9ZSfLwUSfA70oUP2WLaZhy'  
        }

        response = requests.get(url, params=params)


        if response.status_code == 200:

            data = response.json()
            print("Data retrieved successfully:\n")
            df_data = data['response']['data']
            elec_data = pd.DataFrame(df_data)
            elec_data['value'] = pd.to_numeric(elec_data['value'])
            print(elec_data.head())
            # elec_data['period'] = pd.to_datetime(elec_data['period'])
            
            insert_data = list(elec_data.itertuples(index=False, name=None))
            # task_instance = context['task_instance']
            # task_instance.xcom_push(key="the_message", value=insert_data)

            return insert_data
              
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")


    def load_data_to_postgres(**kwargs):
        ti = kwargs['ti']
        insert_data= {{ti.xcom_pull(task_ids='fetch_data_from_api')}}

        print(f"Data pulled from XCom: {insert_data}")
        print(f"Type of data pulled: {type(insert_data)}")

        # db_params = {
        # 'dbname': 'reliance',
        # 'user': 'docker',
        # 'password': 'docker',
        # 'host': "192.168.1.188",#'192.168.1.223',#"192.168.29.101",#
        #     'port': 5432
        # }


        # conn = psycopg2.connect(**db_params)
        # cur = conn.cursor()

        # create_table_query = '''
        # CREATE TABLE IF NOT EXISTS elec_api_data (
        #     period TEXT,
        #     respondent TEXT,
        #     respondent_name TEXT,
        #     type TEXT,
        #     type_name TEXT,
        #     value BIGINT,
        #     value_units TEXT
        # );
        # '''

        # cur.execute(create_table_query)
        # conn.commit()
        # print("Table created")

        # insert_query = '''
        # INSERT INTO elec_api_data (period, respondent, respondent_name, type, type_name, value, value_units)
        # VALUES (%s, %s, %s, %s, %s, %s, %s)
        # '''
        # cur.executemany(insert_query, insert_data)
        # conn.commit()

        # print(f"Inserted rows {len(insert_data)} to postgres")


    fetch_API_data = PythonOperator(
        task_id='fetch_API_data',
        python_callable=fetch_data_from_api,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_postgres,
        provide_context=True,
    )
    
    
    
    fetch_API_data >> load_data 
