import functions as F
import threading

def create_chunks(total): # E
    """
    Creates a list of chunk boundaries for splitting a total into smaller 
    segments.

    Parameters:
    - total (int): The total number that needs to be divided into chunks.

    Returns:
    - list: A list of integers representing the boundaries of each chunk, 
      starting from 0 and ending with the total. The default chunk size is 
      set to 1,000,000.
    """
    chunks = [0]
    chunk_size = 1000000 
    for i in range(chunk_size, total + 1, chunk_size):
        chunks.append(i)
    if total not in chunks:
        chunks.append(total)
        
    return chunks



def thread_executor(en,offsets,apiUrl,params_template,table_name,requiredCol,repColNameWith,db_lock=threading.Lock()): # E
    """
    Executes API requests in parallel using a thread pool and inserts the 
    retrieved data into a PostgreSQL database.

    Parameters:
    - en: The SQLAlchemy engine object used to connect to the PostgreSQL database.
    - offsets (range): A range of offsets for pagination in API requests.
    - apiUrl (str): The URL of the API to fetch data from.
    - params_template (dict): A dictionary template of parameters for the API request.
    - table_name (str): The name of the table in the PostgreSQL database where 
      the data will be inserted.
    - requiredCol (list): A list of columns to extract from the API response.
    - repColNameWith (str): The new name for the 'value' column in the DataFrame.
    - db_lock (threading.Lock): A lock to ensure thread-safe database operations.

    Note:
    - The function employs a sleep mechanism to manage the request rate to 
      avoid overloading the API.
    """
    with F.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_offset = {}
        
        for offset in offsets:
            params = params_template.copy()
            params['offset'] = offset
            future = executor.submit(F.requests.get, apiUrl, params)
            future_to_offset[future] = offset

        for future in F.as_completed(future_to_offset):
            offset = future_to_offset[future]
            response = future.result()
            try:
                response.raise_for_status()  
                data = response.json()
                df = data['response']['data']
                df = F.pd.DataFrame(df)
                df['value'] = F.pd.to_numeric(df['value'], errors='coerce')  

                df = df[requiredCol].rename(columns={'value': repColNameWith})

                with db_lock:
                    df.to_sql(table_name, en, if_exists='append', index=False)
                    F.time.sleep(1)
                    print(f"Data loaded successfully for {table_name} with offset {offset}")

            except Exception as e:
                error_message = str(e)
                df = F.pd.DataFrame({'table': [table_name], 'offset': [offset], 'error': [error_message]})
                df.to_sql('Failed_import_api', en, if_exists='append', index=False)
                print(f"An error occurred for {table_name} at Offset {offset}: {error_message}")



def apiToPostgres(list_praser,en,apiUrl,params,table_name,requiredCol,repColNameWith):
    """
    Fetches data from an API in batches and inserts it into a PostgreSQL database.

    Parameters:
    - list_praser (list): A list of integers defining the start and end points.
    - en: The SQLAlchemy engine object used to connect to the PostgreSQL database.
    - apiUrl (str): The URL of the API from which data will be fetched.
    - params (dict): A dictionary of parameters to be included in the API request.
    - table_name (str): The name of the table in the PostgreSQL database where 
      the data will be inserted.
    - requiredCol (list): A list of columns that are required for insertion.
    - repColNameWith (dict): To replace the "value" column with desried name e.g."Generation_MWh" 
    """
    for i in range(len(list_praser)-1):
        offsets = range(list_praser[i], list_praser[i+1], 5000)
        thread_executor(en,offsets,apiUrl,params,table_name,requiredCol,repColNameWith)
