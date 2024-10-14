from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from sqlalchemy import create_engine
import pandas as pd
import hvac
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading


def schema_co2_emission(df):
    df = df[['period','stateId','fuelId','value']].rename(columns={'value': 'CO2_Emission_MMT'})
    return df

def schema_energy_generation(df):
    df = df[['period','fueltype','respondent','value']].rename(columns={'value': 'generation_MWh'})
    return df

def schema_renewable_capcity(df):
    df = df[['period','countryRegionId','productName','value']].rename(columns={'value': 'CO2_Emission_MK'})
    return df

def fueltype_calculation(row):
    value = float(row['generation_MWh'])  
    if row['fueltype'] == 'COL':
        return value * 1.03
    elif row['fueltype'] == 'NG':
        return value * 0.42
    elif row['fueltype'] == 'PE':
        return value * 0.93
    else:
        return 0
    

def base_transform(table):
    query = f'SELECT * FROM "{table}";'  
    df = pd.read_sql(query, engine)
    df = df.drop_duplicates()
    return df


def WAEF_cal(eng_gen):
    non_renew_mwh = sum(eng_gen[eng_gen['fueltype'].isin(['COL','NG','OIL'])]['generation_MWh'])
    col_ef = round((sum(eng_gen[eng_gen['fueltype']=='COL']['generation_MWh'])/non_renew_mwh)*1.03,4)
    ng_ef = round((sum(eng_gen[eng_gen['fueltype']=='NG']['generation_MWh'])/non_renew_mwh)*0.42,4)
    oil_ef = round((sum(eng_gen[eng_gen['fueltype']=='OIL']['generation_MWh'])/non_renew_mwh)*0.93,4)
    return col_ef+ng_ef+oil_ef

def create_chunks(total):
    chunks = [0]
    chunk_size = 1000000 
    for i in range(chunk_size, total + 1, chunk_size):
        chunks.append(i)
    if total not in chunks:
        chunks.append(total)
        
    return chunks


def thread_executor(offsets,value,params,key,db_lock=threading.Lock()):
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_offset = {}
        
        for offset in offsets:
            params['offset'] = offset
            future = executor.submit(requests.get, value, params)
            future_to_offset[future] = offset

        for future in as_completed(future_to_offset):
            offset = future_to_offset[future]
            response = future.result()


            if response.status_code == 200:
                data = response.json()
                df = data['response']['data']
                df = pd.DataFrame(df)
                df['value'] = pd.to_numeric(df['value'], errors='coerce')  

                if key == tables[0]:
                    df = schema_co2_emission(df)
                if key == tables[1]:
                    df = schema_energy_generation(df)
                if key == tables[2]:
                    df = schema_renewable_capcity(df)
                with db_lock:
                    df.to_sql(key, engine, if_exists='append', index=False)
                    time.sleep(0.5)
                print(f"Data loaded successfully for {key} with offset {offset}")

            else:
                df = pd.DataFrame({'table': [key], 'offset': [offset], 'error': [response.status_code]})
                df.to_sql('Failed_import_api', engine, if_exists='append', index=False)
                print(f"Failed to retrieve data for {key}. At Offset {offset} with Status code: {response.status_code}")


def EXTRACT():
    base_url = 'https://api.eia.gov/v2/'
    apis = {
        tables[0]: f"{base_url}co2-emissions/co2-emissions-aggregates/data/?frequency=annual",
        tables[1]: f"{base_url}electricity/rto/daily-fuel-type-data/data/?frequency=daily",
        tables[2]: f"{base_url}international/data/?frequency=annual&facets[activityId][]=12&facets[activityId][]=7&facets[productId][]=29&facets[countryRegionId][]=USA"
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


        list_praser = create_chunks(total_record)
        for i in range(len(list_praser)-1):
            offsets = range(list_praser[i], list_praser[i+1], 5000)
            thread_executor(offsets,value,params,key)


def TRANSFORM():
    co2_emi = base_transform(tables[0])
    eng_gen = base_transform(tables[1])
    Ren_cap = base_transform(tables[2])

    def co2_reduction_cal(row):
        value = float(row['generation_MWh'])  
        if row['fueltype'] in ['WAT','SUN','WND']:
            return value * WAEF
        else:
            return 0
        
    
    WAEF = WAEF_cal(eng_gen)
    
    eng_gen['period'] = pd.to_datetime(eng_gen['period'])
    eng_gen['co2_emission_tons'] = eng_gen.apply(fueltype_calculation, axis=1)


    eng_gen['co2_reduction_tons']= eng_gen.apply(co2_reduction_cal,axis=1)

    years = list(eng_gen['period'].dt.year.unique())
    for year in years:
        df = eng_gen[eng_gen['period'].dt.year == year]
        df.to_sql(f'eng_gen_{year}' , engine_sink, if_exists='append', index=False)





default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),  
    'retries': 1,                         
}

with DAG(
    'PIPELINE',                   
    default_args=default_args,
    schedule_interval='@daily',          
    catchup=False,                       
) as dag:

    vault_address = Variable.get("vault_address")
    vault_token = Variable.get("vault_token")
    secret_path = Variable.get("secret_path")

    client = hvac.Client(url=vault_address, token=vault_token)
    read_secret_result  = client.read(secret_path)
    location = read_secret_result['data']['data']

    API_KEY=location['API_KEY']

    db_params = {
    'dbname': location['DB'],
    'user': location['POST_USER'],
    'password': location['POST_PASS'],
    'host': location['HOST'],
    'port':location['PORT']
    }
    
    engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
    engine_sink = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{location['DB_SINK']}")
    
    tables = ['co2_emi_api','eng_gen_api','Ren_cap_api']
    
    EXTRACT_API_DATA = PythonOperator(
        task_id='EXTRACT',      
        python_callable=EXTRACT,      
    )

    TRANSFORM_DATA = PythonOperator(
        task_id='TRANSFORM',      
        python_callable=TRANSFORM,      
    )


    EXTRACT_API_DATA >> TRANSFORM_DATA