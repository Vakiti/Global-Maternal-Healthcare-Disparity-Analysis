## Using API to get data
# # make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from pathlib import Path
import json
import requests
from datetime import date

def extract_data(target_dir):
    print('Extracting...')
    neonatal_ind_code = 'nmr'
    skilled_health_ind_code = 'MDG_0000000025'
    maternal_mortality_ind_code = 'MDG_0000000026'
    antenatal_ind_code = 'WHS4_154'

    ind_codes = [neonatal_ind_code, skilled_health_ind_code, maternal_mortality_ind_code, antenatal_ind_code]
    csv_names = ['neonatal', 'skilled_health', 'maternal_mortality', 'antenatal']

    BASE_URL = 'https://ghoapi.azureedge.net/api/'
    #DATE_2000S = '?$filter=date(TimeDimensionBegin) ge 2000-01-01'

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    for code, name in zip(ind_codes, csv_names):
        url = BASE_URL + code
        response = requests.get(url)
        # make sure we got a valid response
        if(response.ok):
            json_data = response.json()
            df = pd.DataFrame(json_data['value'])
            df.to_csv(target_dir + name + str(date.today()) + '.csv', index=False)
        else:
            print('Error Extracting From API')

    print('Extraction Complete')



    # if str(date) != start_date:
    #     results_df['datetime'] = pd.to_datetime(results_df['datetime'])
    #     results_df = results_df[results_df['datetime'].dt.strftime('%Y-%m-%d') == str(date)]

extract_data('API_test/')