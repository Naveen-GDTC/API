TABLES = ['co2_emi_api','eng_gen_api','ren_cap_api']

base_url = 'https://api.eia.gov/v2/'


APIS = {
    TABLES[0]: 
        [f"{base_url}co2-emissions/co2-emissions-aggregates/data/", # URL
         ['period','stateId','fuelId','value'], # Columns that are required
         'CO2_Emission_MMT', # name to be replaced with "value" column name
         {"frequency":"annual", # parameters
         "sort[0][column]":"period",
         "sort[0][direction]":"asc",
          'data[0]':'value'}],

    TABLES[1]: 
        [f"{base_url}electricity/rto/daily-fuel-type-data/data/", # URL
         ['period','fueltype','respondent','value'], # Columns that are required
         'generation_MWh', # name to be replaced with "value" column name
         {"frequency":"daily",# parameters
          "sort[0][column]":"period",
          "sort[0][direction]":"asc", 
          'data[0]':'value'}],

    TABLES[2]: 
        [f"{base_url}international/data/", # URL
         ['period','countryRegionId','productName','value'], # Columns that are required
         'capacity_MK', # name to be replaced with "value" column name
         {"frequency":"annual", # parameters
          'data[0]':'value',
          "sort[0][column]":"period",
          "sort[0][direction]":"asc",
          "facets[activityId][]":7,
          "facets[productId][]":[116,29,33,37],
          "facets[countryRegionId][]":"USA"}]
}