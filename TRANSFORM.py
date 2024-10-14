from sqlalchemy import create_engine
import pandas as pd
import secret


db_params = {
'dbname': secret.DB,
'user': secret.POST_USER,
'password': secret.POST_PASS,
'host': secret.HOST,
'port': secret.PORT
}


conn_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
engine = create_engine(conn_string)

tables = ['co2_emi_api','eng_gen_api','Ren_cap_api']


def base_transform(table):
    query = f'SELECT * FROM "{table}";'  
    df = pd.read_sql(query, engine)
    df = df.drop_duplicates()
    return df


co2_emi = base_transform(tables[0])
eng_gen = base_transform(tables[1])
Ren_cap = base_transform(tables[2])



###########################################################################################################
# TRANSFORMATION

# eng_gen['value'] = pd.to_numeric(eng_gen['value'],errors='coerce')
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


def co2_reduction_cal(row):
    value = float(row['value'])  
    if row['fueltype'] in ['WAT','SUN','WND']:
        return value * WAEF
    else:
        return 0




eng_gen['co2_emission_tons'] = eng_gen.apply(fueltype_calculation, axis=1)

# Energy generation from non-renewable sources 
non_renew_mwh = sum(eng_gen[eng_gen['fueltype'].isin(['COL','NG','OIL'])]['value'])

# sum of energy generated from col divided by total non-renewable energy generation times col emissions factor 
# similar doing it for natrual gas and oil then summing all three to get weighted average
col_ef = round((sum(eng_gen[eng_gen['fueltype']=='COL']['value'])/non_renew_mwh)*1.03,4)
ng_ef = round((sum(eng_gen[eng_gen['fueltype']=='NG']['value'])/non_renew_mwh)*0.42,4)
oil_ef = round((sum(eng_gen[eng_gen['fueltype']=='OIL']['value'])/non_renew_mwh)*0.93,4)

# Weighted Average Emission Factor
WAEF = col_ef+ng_ef+oil_ef


eng_gen['co2_reduction']= eng_gen.apply(co2_reduction_cal,axis=1)

print(eng_gen)
