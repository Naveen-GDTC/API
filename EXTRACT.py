import requests
from sqlalchemy import create_engine
import pandas as pd
import secret
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time


db_lock = threading.Lock()
base_url = 'https://api.eia.gov/v2/'
tables = ['co2_emi_api','eng_gen_api','Ren_cap_api']
apis = {
    # "co2_emi_api": f"{base_url}co2-emissions/co2-emissions-aggregates/data/?frequency=annual",
    "eng_gen_api": f"{base_url}electricity/rto/daily-fuel-type-data/data/?frequency=daily",
    # "Ren_cap_api": f"{base_url}international/data/?frequency=annual&facets[activityId][]=7&facets[productId][]=116&facets[productId][]=29&facets[productId][]=33&facets[productId][]=37&facets[countryRegionId][]=USA"
    #renewable : &facets[activityId][]=7&facets[productId][]=116&facets[productId][]=29&facets[productId][]=33&facets[productId][]=37&facets[countryRegionId][]=USA
}
def schema_co2_emission(df):
    df = df[['period','stateId','fuelId','value']].rename(columns={'value': 'CO2_Emission_MMT'})
    return df

def schema_energy_generation(df):
    df = df[['period','fueltype','respondent','value']].rename(columns={'value': 'generation_MWh'})
    return df

def schema_renewable_capcity(df):
    df = df[['period','countryRegionId','productName','value']].rename(columns={'value': 'CO2_Emission_MK'})
    return df

def create_chunks(total):
    chunks = [0]
    chunk_size = 1000000 

    for i in range(chunk_size, total + 1, chunk_size):
        chunks.append(i)

    if total not in chunks:
        chunks.append(total)

    return chunks

def thread_executor(offsets):
    with ThreadPoolExecutor(max_workers=5) as executor:
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

for key, value in apis.items():
    print(key)
    offset = 0 
    params = {
        'data[0]':'value',
        'length': 5000,
        'api_key':secret.API_KEY 
    }

    #Getting the total records
    params['offset'] = offset
    response = requests.get(value, params=params)
    json_data = response.json()
    total_record = int(json_data['response']['total'])




    db_params = {
    'dbname': secret.DB,
    'user': secret.POST_USER,
    'password': secret.POST_PASS,
    'host': secret.HOST,
    'port': secret.PORT
    }
    
    engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")


    list_praser = create_chunks(total_record)
    for i in range(len(list_praser)-1):
        offsets = range(list_praser[i], list_praser[i+1], 5000)
        thread_executor(offsets)


    # while offset < total_record: #no_rec: #
    #     params['offset'] = offset
    #     response = requests.get(value, params=params)
        
    #     if response.status_code == 200:
    #         data = response.json()
    #         df = data['response']['data']
    #         df = pd.DataFrame(df)
    #         df['value'] = pd.to_numeric(df['value'],errors='coerce')

    #         if key == tables[0]:
    #             df = schema_co2_emission(df)
    #         if key == tables[1]:
    #             df = schema_energy_generation(df)
    #         if key == tables[2]:
    #             df = schema_renewable_capcity(df) 

    #         df.to_sql(key , engine, if_exists='append', index=False)
    #         print(f"Data loaded successfully for {key} with offset {offset}")
        
    #     else:
    #         df = pd.DataFrame({'table':[key],'offset':[offset],'error':[response.status_code]})
    #         df.to_sql('Failed_import_api' , engine, if_exists='append', index=False)
    #         print(f"Failed to retrieve data for {key}. At Offset {offset} with Status code: {response.status_code}")
        
    #     offset += 5000
