TABLES = ['co2_emi_api','eng_gen_api','Ren_cap_api']

base_url = 'https://api.eia.gov/v2/'


APIS = {
    TABLES[0]: 
        [f"{base_url}co2-emissions/co2-emissions-aggregates/data/?frequency=annual",
         ['period','stateId','fuelId','value'],
         'CO2_Emission_MMT'],

    TABLES[1]: 
        [f"{base_url}electricity/rto/daily-fuel-type-data/data/?frequency=daily",
         ['period','fueltype','respondent','value'],
         'generation_MWh'],

    TABLES[2]: 
        [f"{base_url}international/data/?frequency=annual&facets[activityId][]=7&facets[productId][]=116&facets[productId][]=29&facets[productId][]=33&facets[productId][]=37&facets[countryRegionId][]=USA",
         ['period','countryRegionId','productName','value'],
         'Generation_MK']
}