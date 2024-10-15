import functions as F
import threading

def create_chunks(total): # E
    chunks = [0]
    chunk_size = 1000000 
    for i in range(chunk_size, total + 1, chunk_size):
        chunks.append(i)
    if total not in chunks:
        chunks.append(total)
        
    return chunks

def thread_executor(en,offsets,apiUrl,params,table_name,requiredCol,repColNameWith,db_lock=threading.Lock()): # E
    print(f'requiredCol : {requiredCol}')
    with F.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_offset = {}
        
        for offset in offsets:
            params['offset'] = offset
            future = executor.submit(F.requests.get, apiUrl, params)
            future_to_offset[future] = offset

        for future in F.as_completed(future_to_offset):
            offset = future_to_offset[future]
            response = future.result()


            if response.status_code == 200:
                data = response.json()
                df = data['response']['data']
                df = F.pd.DataFrame(df)
                df['value'] = F.pd.to_numeric(df['value'], errors='coerce')  
                df = df[requiredCol].rename(columns={'value': repColNameWith})
                with db_lock:
                    df.to_sql(table_name, en, if_exists='append', index=False)
                    F.time.sleep(0.5)
                print(f"Data loaded successfully for {table_name} with offset {offset}")

            else:
                df = F.pd.DataFrame({'table': [table_name], 'offset': [offset], 'error': [response.status_code]})
                df.to_sql('Failed_import_api', en, if_exists='append', index=False)
                print(f"Failed to retrieve data for {table_name}. At Offset {offset} with Status code: {response.status_code}")


def apiToPostgres(list_praser,en,apiUrl,params,table_name,requiredCol,repColNameWith):
    print(f'requiredCol : {requiredCol}')
    for i in range(len(list_praser)-1):
        offsets = range(list_praser[i], list_praser[i+1], 5000)
        thread_executor(en,offsets,apiUrl,params,table_name,requiredCol,repColNameWith)





