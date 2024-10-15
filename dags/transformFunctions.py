import functions as F

def fueltype_calculation(row): # T
    value = float(row['generation_MWh'])  
    if row['fueltype'] == 'COL':
        return value * 1.03
    elif row['fueltype'] == 'NG':
        return value * 0.42
    elif row['fueltype'] == 'PE':
        return value * 0.93
    else:
        return 0
    

def baseTransform(table, engine): # T
    print(table)
    query = f'SELECT * FROM "{table}";'  
    df = F.pd.read_sql(query, engine)
    df = df.drop_duplicates()
    return df


def WAEF_cal(eng_gen): # T
    non_renew_mwh = sum(eng_gen[eng_gen['fueltype'].isin(['COL','NG','OIL'])]['generation_MWh'])
    col_ef = round((sum(eng_gen[eng_gen['fueltype']=='COL']['generation_MWh'])/non_renew_mwh)*1.03,4)
    ng_ef = round((sum(eng_gen[eng_gen['fueltype']=='NG']['generation_MWh'])/non_renew_mwh)*0.42,4)
    oil_ef = round((sum(eng_gen[eng_gen['fueltype']=='OIL']['generation_MWh'])/non_renew_mwh)*0.93,4)
    return col_ef+ng_ef+oil_ef

def co2_reduction_cal(row,multiplier): # T
    value = float(row['generation_MWh'])  
    if row['fueltype'] in ['WAT','SUN','WND']:
        return value * multiplier
    else:
        return 0