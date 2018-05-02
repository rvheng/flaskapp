import os
import pandas
from sqlalchemy import create_engine
import sqlalchemy
from _mysql_exceptions import DataError
from sqlalchemy.exc import DataError
from sqlalchemy.exc import OperationalError
import ntpath
import MySQLdb


class DataImport:

    def __init__(self):
        self.current_directory = os.getcwd()
        self.data_directory = os.path.join(self.current_directory, "raw_data")
        self.data_directory_asm = os.path.join(self.data_directory, "asm_data")
        self.data_directory_gasoline = os.path.join(self.data_directory, "gasoline_prices")
        self.processed_directory = os.path.join(self.current_directory, "processed_data")
        self.table_prefix = "dataset_"
        self.database_schema = "myflaskapp"
        self.database_user = "root"
        self.database_pw = "password"
        self.meta_data_tbl = "data_sources_meta_data"
        self.correlations_table = "correlations"
        self.data_sources_meta_data_table = "data_sources_meta_data"

    def get_current_directory(self):
        return self.current_directory

    def get_data_directory(self, type="Default"):
        if type == "Default":
            return self.data_directory
        elif type == "asm":
            return self.data_directory_asm
        elif type == "gasoline_prices":
            return self.data_directory_gasoline

    def get_processed_directory(self):
        return self.processed_directory

    def move_file(self, source_file_path, dest_file_path):
        return os.rename(source_file_path, dest_file_path)

    def get_csv_files_in_directory(self, directory):
        csvs = []
        for file in os.listdir(directory):
            name, extension = file.split(".")
            if extension == "csv":
                csvs.append(file)

        return csvs

    def sql_col(self, dfparam):

        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                max_len = int(dfparam[i].str.encode(encoding='utf-8').str.len().max())

                dtypedict.update({i: sqlalchemy.types.VARCHAR(length=max_len)})

            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy.types.DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy.types.INT()})

        return dtypedict

    def sql_string_col(self, dfparam):

        arr = []
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                arr.append(i)
        return arr

    def init_database(self):

        # Connect to the database with no schema specified
        engine = create_engine('mysql://{0}:{1}@localhost'.format(self.database_user, self.database_pw))

        # Create the schema if it does not already exist
        engine.execute("CREATE DATABASE IF NOT EXISTS {0} ".format(self.database_schema))

        # Create the data_sources_meta_data table if not exists
        engine.execute(
            'CREATE TABLE IF NOT EXISTS {0}.data_sources_meta_data (tbl_name varchar(500),val varchar(500),min_year varchar(4),max_year varchar(4),val_desc varchar(1000),source varchar(300),PRIMARY KEY (tbl_name))'.format(
                self.database_schema))

        engine.execute(
            'CREATE TABLE IF NOT EXISTS {0}.correlations (dataset1_table_name varchar(500),dataset2_table_name varchar(500),correlation_coefficient decimal(4,4),PRIMARY KEY (dataset1_table_name,dataset2_table_name))'.format(
                self.database_schema))

        # Create the correlations table if not already exist

        # Get the database engine for our schema
        engine = create_engine(
            'mysql://{0}:{1}@localhost/{2}?charset=utf8mb4'.format(self.database_user, self.database_pw,
                                                                   self.database_schema))

        return engine

    def import_pre_process(self, csv, data_source_type, engine):
        # Here we take each csv file that will be imported and we extract the metadata
        # We need to extract the min_year, max_year, val, val_desc
        # data_source should be an os path to the csv

        print("Processing: ", csv)

        csv_file_name = ntpath.basename(csv)

        print("basename: ", csv_file_name)

        table_name, extension = csv_file_name.split('.')

        table_name = table_name.lower()
        print("table name:", table_name)

        if data_source_type == "asm":
            # Read CSV into Pandas
            df = pandas.read_csv(csv)

            # Get the Metadata
            max_year = df['YEAR'].max()
            min_year = df['YEAR'].min()
            value = df['PSCODE'].min()
            value = "VAL_" + str(value)
            value_desc = df['PSCODE_TTL'].min()
            table_name = "dataset_" + str(table_name)

            # Insert Meta Data
            engine.execute(
                "INSERT INTO {0}.{1} (tbl_name,val,min_year,max_year,val_desc,source) values ('{2}','{3}','{4}','{5}','{6}','{7}')".format(
                    self.database_schema, self.meta_data_tbl,
                    table_name, value, min_year, max_year,
                    value_desc, "census"))

            # Rename val column
            df.rename(columns={"PRODVAL": value}, inplace=True)

            # Drop Unnecessary Columns
            delete_columns = ['PSCODE', 'PSCODE_TTL', 'time', 'PSCODE.1', 'us']
            delete_columns = [c for c in delete_columns if c in df.columns]
            df.drop(delete_columns, axis=1, inplace=True)

            print("Processed DF: ", df)

            return df
        elif data_source_type == "gasoline_prices":
            # Read CSV into Pandas
            df = pandas.read_csv(csv)

            # Get the Metadata
            max_year = df['YEAR'].max()
            min_year = df['YEAR'].min()

            # The value will be the column other than YEAR
            value = [x for x in list(df.columns) if x not in ["YEAR"]]

            value = value[0]

            original_column_name = value

            # The description will be the raw string values in the column header
            value_desc = str(value)

            # The value will the the string with spaces replaced
            value = value.replace(" ", "_")

            value = "VAL_" + str(value)

            # Rename val column
            df.rename(columns={original_column_name: value}, inplace=True)

            print("original column name: ", original_column_name)

            print("new column name: ", value)

            # Table name will be prepended with datasource
            table_name = "dataset_" + str(table_name)

            # Insert Meta Data
            engine.execute(
                "INSERT INTO {0}.{1} (tbl_name,val,min_year,max_year,val_desc,source) values ('{2}','{3}','{4}','{5}','{6}','{7}')".format(
                    self.database_schema, self.meta_data_tbl,
                    table_name, value, min_year, max_year,
                    value_desc, "AAA"))

            return df

    def import_data(self, df, csv, engine, table_name, drop_strings=False):
        # csv should be a full path

        string_columns = self.sql_string_col(df)

        if drop_strings:
            print("dropping string columns: ", string_columns)

            df.drop(string_columns, axis=1, inplace=True)

        print("Creating Database Table: " + table_name)

        output_dict = self.sql_col(df)

        print("Created Database Table: " + table_name)

        try:

            df.to_sql(self.table_prefix + table_name.lower(), engine, if_exists="replace", dtype=output_dict,
                      index=False)

            self.move_file(os.path.join(self.get_data_directory(), csv),
                           os.path.join(self.get_processed_directory(), csv))

            print("Moved File From: " + os.path.join(self.get_data_directory(), csv) + " To: " + os.path.join(
                self.get_processed_directory(), csv))
        except DataError as e:
            print(e.orig)
            print("Did Not Move File: " + os.path.join(self.get_data_directory(), csv))
        except OperationalError as e:
            print(e.orig)
            self.move_file(os.path.join(self.get_data_directory(), csv),
                           os.path.join(self.get_processed_directory(), csv))

            print("Moved File From: " + self.get_data_directory() + csv + " To: " + self.get_processed_directory())

    def import_csvs(self, data_type):

        csvs = self.get_csv_files_in_directory(self.get_data_directory(data_type))

        engine = self.init_database()

        for csv in csvs:
            # table name will be the name of the csv file
            table_name, extension = csv.split(".")

            # Get full path to csv file.
            csv = os.path.join(self.get_data_directory(data_type), csv)

            # Pre-Process the CSV files.
            # Here add entries to the data_sources_meta_data table.
            # Remove columns from the data which are not needed.
            # Format the data so we have a YEAR column
            filtered_data_frame = self.import_pre_process(csv, data_type, engine)

            # Import the csv.
            self.import_data(filtered_data_frame, csv, engine, table_name, drop_strings=False)

    def insert_correlation(self, table_one, table_two, correlation_coefficient):

        # Connect to the database with no schema specified
        engine = create_engine('mysql://{0}:{1}@localhost'.format(self.database_user, self.database_pw))

        sql = "INSERT IGNORE INTO {0}.{1} (dataset1_table_name,dataset2_table_name,correlation_coefficient) VALUES ('{2}','{3}',{4})".format(
            self.database_schema, self.correlations_table, table_one,
            table_two, correlation_coefficient)

        # Create the schema if it does not already exist
        engine.execute(sql)

    def map_val_to_table_name(self, val):

        # Initialize Database Connection
        db = MySQLdb.connect(host="localhost", user=self.database_user, passwd=self.database_pw,
                             db=self.database_schema)

        sql = "SELECT tbl_name FROM {0}.{1} WHERE val ='{2}'".format(
            self.database_schema, self.data_sources_meta_data_table, val)

        db.query(sql)

        # Store the result
        result = db.store_result()

        # Fetch all the result rows
        row = result.fetch_row(maxrows=1, how=1)

        result = row[0].get('tbl_name')

        return result
