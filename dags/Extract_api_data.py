from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from sqlalchemy import create_engine
import pandas as pd
import hvac





def EXTRACT():
    client = hvac.Client(url=vault_address, token=vault_token)
    res = client.is_authenticated()
    read_secret_result  = client.read('secret/data/cred')
    API_KEY=read_secret_result['data']['data']['API_KEY']

    base_url = 'https://api.eia.gov/v2/'
    apis = {
        "co2_emi_api": f"{base_url}co2-emissions/co2-emissions-aggregates/data/?frequency=annual",
        "eng_gen_api": f"{base_url}electricity/rto/daily-fuel-type-data/data/?frequency=daily",
        "Ren_cap_api": f"{base_url}international/data/?frequency=annual&facets[activityId][]=12&facets[activityId][]=7&facets[productId][]=29&facets[countryRegionId][]=USA"
    }

    for key, value in apis.items():
        print(key)
        offset = 0 
        params = {
            'data[0]':'value',
            'length': 5000,
            'api_key':API_KEY 
        }

        #Getting the total records
        params['offset'] = offset
        response = requests.get(value, params=params)
        json_data = response.json()
        total_record = int(json_data['response']['total'])

        db_params = {
        'dbname': read_secret_result['data']['data']['DB'],
        'user': read_secret_result['data']['data']['POST_USER'],
        'password': read_secret_result['data']['data']['POST_PASS'],
        'host': read_secret_result['data']['data']['HOST'],
        'port': read_secret_result['data']['data']['PORT']
        }
        
        engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

        no_rec = 0
        if total_record>20001:
            no_rec = 20001
        else:
            no_rec = total_record

        while offset < no_rec: #total_record:
            params['offset'] = offset
            response = requests.get(value, params=params)
            offset += 5000
            if response.status_code == 200:
                data = response.json()
                df = data['response']['data']
                df = pd.DataFrame(df)
                df['value'] = pd.to_numeric(df['value'],errors='coerce')
                # df.to_sql(key , engine, if_exists='append', index=False)

                print(f"Data loaded successfully for {key} with offset {offset}:\n")
            else:
                print(f"Failed to retrieve data for {key}. Status code: {response.status_code}")
                break


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),  
    'retries': 1,                         
}

with DAG(
    'ETL_API_DATA',                   
    default_args=default_args,
    schedule_interval='@daily',          
    catchup=False,                       
) as dag:

    vault_address = Variable.get("vault_address")
    vault_token = Variable.get("vault_token")
   
    EXTRACT_API_DATA = PythonOperator(
        task_id='EXTRACT',      
        python_callable=EXTRACT,      
    )


    EXTRACT_API_DATA