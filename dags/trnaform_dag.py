from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from sqlalchemy import create_engine
import pandas as pd
import hvac

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),  
    'retries': 1,                         
}

def fueltype_calculation(row):
    value = float(row['value'])  
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
    non_renew_mwh = sum(eng_gen[eng_gen['fueltype'].isin(['COL','NG','OIL'])]['value'])
    col_ef = round((sum(eng_gen[eng_gen['fueltype']=='COL']['value'])/non_renew_mwh)*1.03,4)
    ng_ef = round((sum(eng_gen[eng_gen['fueltype']=='NG']['value'])/non_renew_mwh)*0.42,4)
    oil_ef = round((sum(eng_gen[eng_gen['fueltype']=='OIL']['value'])/non_renew_mwh)*0.93,4)
    return col_ef+ng_ef+oil_ef

def TRANSFORM():

    tables = ['co2_emi_api','eng_gen_api','Ren_cap_api']

    co2_emi = base_transform(tables[0])
    eng_gen = base_transform(tables[1])
    Ren_cap = base_transform(tables[2])

    def co2_reduction_cal(row):
        value = float(row['value'])  
        if row['fueltype'] in ['WAT','SUN','WND']:
            return value * WAEF
        else:
            return 0
        
    
    WAEF = WAEF_cal(eng_gen)
    
    eng_gen['period'] = pd.to_datetime(eng_gen['period'])
    eng_gen['year'] = eng_gen['period'].dt.year
    eng_gen['co2_emission_tons'] = eng_gen.apply(fueltype_calculation, axis=1)


    eng_gen['co2_reduction']= eng_gen.apply(co2_reduction_cal,axis=1)

    years = list(eng_gen['year'].unique())
    for year in years:
        df = eng_gen[eng_gen['year'] == year]
        df.to_sql(f'eng_gen_{year}' , engine_sink, if_exists='append', index=False)



with DAG(
    'TRANSFORM_DATA',                   
    default_args=default_args,
    schedule_interval='@daily',          
    catchup=False,                       
) as dag:

    vault_address = Variable.get("vault_address")
    vault_token = Variable.get("vault_token")

    client = hvac.Client(url=vault_address, token=vault_token)
    res = client.is_authenticated()
    read_secret_result  = client.read('secret/data/cred')

    location =read_secret_result['data']['data']

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
   
    TRANSFORM_DATA = PythonOperator(
        task_id='TRANSFORM',      
        python_callable=TRANSFORM,      
    )


    TRANSFORM_DATA