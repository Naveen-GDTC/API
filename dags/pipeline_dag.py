import functions as F
import extractFunctions as eF
import transformFunctions as tF
import apiUrl as aU
        

def EXTRACT():
    for table_name, req  in aU.APIS.items():
        print(table_name)
        params = req[3]
        offset = eF.IF_EXISTS(table_name,engine)

        params['api_key'] = location['API_KEY']
        params['offset'] = offset

        response = F.requests.get(req[0], params=params)
        json_data = response.json()
        total_record = int(json_data['response']['total'])
        print(total_record)

        list_praser = eF.create_chunks(total_record,offset)
        if len(list_praser) > 1:
            reqCol = req[1]
            repColNameWith = req[2]
            eF.apiToPostgres(list_praser,engine,req[0],params,table_name,reqCol,repColNameWith)
        else:
            print("No new records were discovered.")


def TRANSFORM(): 
    co2_emi = tF.baseTransform(tables[0],engine)
    eng_gen = tF.baseTransform(tables[1],engine)
    Ren_cap = tF.baseTransform(tables[2],engine)
        
    
    WAEF = tF.WAEF_cal(eng_gen)
    
    eng_gen['period'] = F.pd.to_datetime(eng_gen['period'])  
    eng_gen['co2_emission_tons'] = eng_gen.apply(tF.fueltype_calculation, axis=1)
    eng_gen['co2_reduction_tons']= eng_gen.apply(tF.co2_reduction_cal,axis=1,multiplier=WAEF)

    years = list(eng_gen['period'].dt.year.unique())
    print(years)
    for year in years:
        df = eng_gen[eng_gen['period'].dt.year == year]
        print(year)
        df.to_sql(f'eng_gen_{year}' , engine_sink, if_exists='append', index=False)


default_args = {
    'owner': 'airflow',
    'start_date': F.datetime(2024, 10, 4),  
    'retries': 1,                         
}

with F.DAG(
    'PIPELINE',                   
    default_args=default_args,
    schedule_interval='@daily',          
    catchup=False,                       
) as dag:


    # Fetching the detials of Vault (url,token and path of secret) from airflow variables
    vault_address = F.Variable.get("vault_address")
    vault_token = F.Variable.get("vault_token")
    secret_path = F.Variable.get("secret_path")

    # To access the vault credentials
    client = F.hvac.Client(url=vault_address, token=vault_token)
    read_secret_result  = client.read(secret_path)
    location = read_secret_result['data']['data']
    
    # creating 2 engines first to connect with source database and second to connect with traget database
    engine = F.create_engine(f"postgresql+psycopg2://{location['POST_USER']}:{location['POST_PASS']}@{location['HOST']}:{location['PORT']}/{location['DB']}")
    engine_sink = F.create_engine(f"postgresql+psycopg2://{location['POST_USER']}:{location['POST_PASS']}@{location['HOST']}:{location['PORT']}/{location['DB_SINK']}")
    
    tables = aU.TABLES
    
    EXTRACT_API_DATA = F.PythonOperator(
        task_id='EXTRACT',      
        python_callable=EXTRACT,      
    )

    TRANSFORM_DATA = F.PythonOperator(
        task_id='TRANSFORM',      
        python_callable=TRANSFORM,      
    )



    EXTRACT_API_DATA >> TRANSFORM_DATA