import json
import requests
import os
import pprint
import re
import json
import pandas as pd

# Get the Project Directory
project_directory = os.path.dirname(os.getcwd())

# Create the meta_data directory if it does not exist.
if not os.path.exists(os.path.join(project_directory, "meta_data")):
    os.makedirs(os.path.join(project_directory, "meta_data"))

meta_data_directory = os.path.join(project_directory, "meta_data")

# Create the raw_data directory if it does not exist.
if not os.path.exists(os.path.join(project_directory, "raw_data")):
    os.makedirs(os.path.join(project_directory, "raw_data"))

raw_data_directory = os.path.join(project_directory, "raw_data")

# Create the asm_data_directory if it does not exist.
if not os.path.exists(os.path.join(raw_data_directory, "asm_data")):
    os.makedirs(os.path.join(raw_data_directory, "asm_data"))

asm_data_directory = os.path.join(raw_data_directory, "asm_data")

url_census_base_url = "https://api.census.gov/data/timeseries/asm/product"

census_variable_list = json.loads(open(os.path.join(meta_data_directory, "asm_product_variables.json")).read())

census_variable_list = ','.join(map(str, [x['Variable'] for x in census_variable_list]))

census_asm_ps_codes = json.loads(open(os.path.join(meta_data_directory, "asm_product_pscodes.json")).read())

census_asm_ps_codes = [X['PS_code'] for X in census_asm_ps_codes]

total_len = len(census_asm_ps_codes)

for number, asm_pscode in enumerate(census_asm_ps_codes,1):
    req_get_columns = "?get="

    req_var_time = "&time=to+2018"

    req_var_for = "&for=us"

    req_asm_pscodes = "&PSCODE=" + asm_pscode

    req_key = "&key=9bb1651c68527c056a2630039dd3d556a4c6010e"

    url = url_census_base_url + req_get_columns + str(
        census_variable_list) + req_var_for + req_var_time + req_asm_pscodes + req_key

    print("downloading [{0} of {1}]: ".format(number,total_len), url)

    # Here we grab the data from the web
    web_response = requests.get(url)

    # Read the JSON result into a Pandas DataFrame
    df = pd.read_json(web_response.text)

    # Set the columns to the value in the first row
    df.rename(columns=df.iloc[0], inplace=True)

    # Drop the first row
    df.drop(df.index[0], inplace=True)

    # CSV File Name
    file_name = "asm_product_" + asm_pscode + ".csv"

    # Save the DataFrame to a CSV
    df.to_csv(os.path.join(asm_data_directory, file_name), index=False)
