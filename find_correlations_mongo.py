from pymongo import MongoClient
import os
import pandas
import ntpath
import re


# Step 1 : Get a list of all the collections

# Step 2 : Filter out the list to contain only the collections which start with dataset_

# Step 3 : Create a new Collection which will hold our YEAR objects.

# Step 4: Each YEAR object will hold individual data points for that year from all data sources.

class DataAggregatorMongo:

    def __init__(self):
        self.document_prefix = "dataset_"
        self.document_prefix_regex = r'^dataset_.*'
        self.app_database = "myflaskapp"
        self.database_user = "flask"
        self.database_pw = "password"
        self.meta_data_document = "data_sources_meta_data"
        self.correlations_document = "correlations"
        self.aggregate_collection = "aggregate_of_data_sets"

        self.mongo_db_server = 'pymongo_test'

        self.server = "localhost"
        self.port = "27017"

    def connect_to_mongo(self):
        conn = MongoClient('mongodb://{0}:{1}'.format(self.server, self.port))
        print("connect_to_mongo() ran")
        return conn

    def get_list_of_dataset_collections(self):
        # Connect to Mongo
        client = self.connect_to_mongo()

        # Specify the database
        db = client['{}'.format(self.app_database)]

        # Get all the collections in the database
        collections = db.list_collection_names()

        # Compile the regex
        r = re.compile(self.document_prefix_regex)

        # Filter out only collections which start with dataset_
        data_set_collections = [x for x in collections if r.match(x)]

        # return the list
        return data_set_collections

    def create_data_aggregation_collection(self):
        client = self.connect_to_mongo()

        # Specify the database
        db = client['{}'.format(self.app_database)]

        # Create the collection if it does not exist
        if self.aggregate_collection not in db.list_collection_names():
            db.create_collection('{}'.format(self.aggregate_collection))

    def insert_into_aggregate_collection(self):
        client = self.connect_to_mongo()

        # Specify the database
        db = client['{}'.format(self.app_database)]

        dataset_collection = db.get_collection('dataset_gasoline_retail_prices_new_york_city_new_york')

        cursor = dataset_collection.find({})
        for document in cursor:
            self.create_year_document(document['YEAR'])
            self.insert_data_point_into_year_document(document)

    def create_year_document(self, year):
        # Connect to Mongo
        client = self.connect_to_mongo()

        # Specify the database
        db = client['{}'.format(self.app_database)]

        # Create the document to be inserted. Note that YEAR will be a string. We may need to change this later
        doc = {'YEAR': year, 'DATA_POINTS': []}

        # Check to see if a YEAR document exists. If it exists we do not create it again.
        count = db['{}'.format(self.aggregate_collection)].count({"YEAR": year})

        if count == 0:
            db['{}'.format(self.aggregate_collection)].insert(doc)

    def insert_data_point_into_year_document(self, document):
        # Connect to Mongo
        client = self.connect_to_mongo()

        # Specify the database
        db = client['{}'.format(self.app_database)]

        re.compile(r'^VAL_.*')

        doc = {}

        # Create the data point to be inserted
        for key, value in document.items():
            if re.match(key):
                doc = {key: value}

        



di = DataAggregatorMongo()

dataset_collections = di.get_list_of_dataset_collections()

di.create_data_aggregation_collection()

# for dataset in dataset_collections:
#   print(dataset)

di.insert_into_aggregate_collection()
