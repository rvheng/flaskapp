from pymongo import MongoClient
import os
import pandas
import ntpath


class DataImportMongo:

    def __init__(self):
        self.current_directory = os.getcwd()
        self.data_directory = os.path.join(self.current_directory, "raw_data")
        self.data_directory_asm = os.path.join(self.data_directory, "asm_data")
        self.data_directory_gasoline = os.path.join(self.data_directory, "gasoline_prices")
        self.processed_directory = os.path.join(self.current_directory, "processed_data")
        self.document_prefix = "dataset_"
        self.collection = "myflaskapp"
        self.database_user = "flask"
        self.database_pw = "password"
        self.meta_data_document = "data_sources_meta_data"
        self.correlations_document = "correlations"

        self.server = "localhost"
        self.port = "27017"

    def get_csv_files_in_directory(self, directory):
        csvs = []
        for file in os.listdir(directory):
            name, extension = file.split(".")
            if extension == "csv":
                csvs.append(file)

        return csvs

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

    def connect_to_mongo(self):
        conn = MongoClient('mongodb://{0}:{1}'.format(self.server, self.port))
        print("connect_to_mongo() ran")
        return conn

    def test(self):
        con = self.connect_to_mongo()

        db = con.pymongo_test

        file_path = os.path.join(self.data_directory_asm, "asm_product_311111W.csv")

        df = pandas.read_csv(file_path)

        data = df.to_dict(orient='records')

        print(df)

        print(data)

    def import_csvs(self, data_type):
        csvs = self.get_csv_files_in_directory(self.get_data_directory(data_type))

        # I don't think this is needed for mongo
        # engine = self.init_database()
        con = self.connect_to_mongo()

        for csv in csvs:
            # table name will be the name of the csv file
            document_name, extension = csv.split(".")

            # Get full path to csv file.
            csv = os.path.join(self.get_data_directory(data_type), csv)

            # Pre-Process the CSV files.
            # Here add entries to the data_sources_meta_data table.
            # Remove columns from the data which are not needed.
            # Format the data so we have a YEAR column
            filtered_data_frame = self.import_pre_process(csv, data_type, con)

            # Import the csv.
            self.import_data(filtered_data_frame, csv, con, document_name)

    def import_pre_process(self, csv, data_source_type, con):
        # Here we take each csv file that will be imported and we extract the metadata
        # We need to extract the min_year, max_year, val, val_desc
        # data_source should be an os path to the csv

        print("Processing: ", csv)

        csv_file_name = ntpath.basename(csv)

        print("basename: ", csv_file_name)

        document_name, extension = csv_file_name.split('.')

        document_name = document_name.lower()

        print("document name:", document_name)

        if data_source_type == "asm":
            # Read CSV into Pandas
            df = pandas.read_csv(csv)

            # Get the Metadata
            max_year = df['YEAR'].max()
            min_year = df['YEAR'].min()
            value = df['PSCODE'].min()
            value = "VAL_" + str(value)
            value_desc = df['PSCODE_TTL'].min()
            document_name = "dataset_" + str(document_name)

            # Prepare data for mongo
            meta_data = {'document_name': document_name, 'val': value, 'min_year': str(min_year),
                         'max_year': str(max_year),
                         'val_desc': value_desc, 'source': data_source_type}

            print(meta_data)

            db = con.myflaskapp

            data_sources_meta_data = db.data_sources_meta_data

            result = data_sources_meta_data.insert_one(meta_data)

            print('One data_sources_meta_data: {0}'.format(result.inserted_id))

            # Rename val column
            df.rename(columns={"PRODVAL": value}, inplace=True)

            # Drop Unnecessary Columns
            delete_columns = ['PSCODE', 'PSCODE_TTL', 'time', 'PSCODE.1', 'us']
            delete_columns = [c for c in delete_columns if c in df.columns]
            df.drop(delete_columns, axis=1, inplace=True)

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
            document_name = "dataset_" + str(document_name)

            # Prepare data for mongo
            meta_data = {'document_name': document_name, 'val': value, 'min_year': str(min_year),
                         'max_year': str(max_year),
                         'val_desc': value_desc, 'source': data_source_type}

            print(meta_data)

            db = con.myflaskapp

            data_sources_meta_data = db.data_sources_meta_data

            result = data_sources_meta_data.insert_one(meta_data)

            print('One data_sources_meta_data: {0}'.format(result.inserted_id))
            return df

    def import_data(self, df, csv, mongo_conn, document_name):
        # csv should be a full path
        con = self.connect_to_mongo()

        db = mongo_conn.myflaskapp

        # Prepend the document name with the prefix
        document_name = self.document_prefix + document_name

        # Create a new document in the database
        new_document = db['{}'.format(document_name)]

        file_path = os.path.join(csv)

        data = df.to_dict(orient='records')

        final_arr = []

        # Pandas was saving numbers as Objects. We need to cast them back to strings
        for dict in data:
            tmp_dict = {}
            for key, value in dict.items():
                print("key: ", key, " value: ", value)
                if key == "YEAR":
                    tmp_dict.update({'{}'.format(key): '{}'.format(str(int(value)))})
                else:
                    tmp_dict.update({'{}'.format(key): '{}'.format(str(value))})
            final_arr.append(tmp_dict)

        print(final_arr)
        new_document.insert_many(final_arr)

        # self.move_file(os.path.join(self.get_data_directory(), csv),
        #               os.path.join(self.get_processed_directory(), csv))

        # print("Moved File From: " + os.path.join(self.get_data_directory(), csv) + " To: " + os.path.join(
        #    self.get_processed_directory(), csv))
