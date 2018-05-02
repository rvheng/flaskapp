import pandas as pd
import os
import pandas
import MySQLdb
import _mysql
from sqlalchemy import create_engine
import sqlalchemy
from data_import_scripts import data_import

# Initialize Database Connection
db = MySQLdb.connect(host="localhost", user="root", passwd="password", db="myflaskapp")

# Select all the data source tables
db.query("SELECT tbl_name FROM data_sources_meta_data")

# Store the result
result = db.store_result()

# Fetch all the result rows
tables = result.fetch_row(maxrows=0)

# Build Natural Join Statement
sql_dataset_natural_join = "SELECT * FROM "

for i, table in enumerate(tables):
    if i == 0:
        sql_dataset_natural_join += " {0}".format(table[0])
    else:
        sql_dataset_natural_join += " natural join {0}".format(table[0])

print(sql_dataset_natural_join)

# Initialize SQL Alchemy Connection for Pandas
di = data_import.DataImport()
eng = di.init_database()

# Read the Natural Join result into the pandas DataFrame
df = pd.read_sql(sql_dataset_natural_join, eng)

# Drop the YEAR column since we don't want to correlate this
df.drop(columns=['YEAR'], inplace=True)

# The next couple of lines I found on stack exchange and modified the code.
correlated_df = df.corr().abs()

unstacked_df = correlated_df.unstack()

sorted_series = unstacked_df.sort_values(ascending=False)

mask = (sorted_series < .9) & (sorted_series > .7)

sorted_series_filtered = sorted_series[mask]

# print(sorted_series_filtered.to_dict())

correlations_table = "correlations"

correlations = sorted_series_filtered.to_dict()

for correlation, correlation_coefficient in correlations.items():
    table1 = di.map_val_to_table_name(correlation[0])
    table2 = di.map_val_to_table_name(correlation[1])
    # Select all the data source tables
    di.insert_correlation(table1, table2, round(correlation_coefficient, 4))

# Step 1: Get a list of all the tables in the data_sources_meta_data table

# Step 2: Write a query which joins all the tables from step 1

# Step 3: Execute the Query from step 2 and load it into a pandas DataFrame

# Step 4: Run the correlation script on the data from step 3.

# Step 5: Save the results from Step 4 into the correlations table
