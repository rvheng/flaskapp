from pymongo import MongoClient
import os
import pandas
import ntpath


# Step 1 : Get a list of all the collections

# Step 2 : Filter out the list to contain only the collections which start with dataset_

# Step 3 : Create a new Document which will hold our YEAR objects.

# Step 4: Each YEAR object will hold individual data points for that year from all data sources.

class DataAggregatorMongo:

    def __init__(self):
        self.document_prefix = "dataset_"
        self.app_database = "myflaskapp"
        self.database_user = "flask"
        self.database_pw = "password"
        self.meta_data_document = "data_sources_meta_data"
        self.correlations_document = "correlations"
        self.aggregate_document = "aggregate_of_data"

        self.mongo_db_server = 'pymongo_test'

        self.server = "localhost"
        self.port = "27017"

    def connect_to_mongo(self):
        conn = MongoClient('mongodb://{0}:{1}'.format(self.server, self.port))
        print("connect_to_mongo() ran")
        return conn

    def get_list_of_collections_starting_with(self,start_with):

        conn = self.connect_to_mongo()

        server_name = self.mongo_db_server

        db = conn['{}'.format(server_name)]

        flask_database = db['{}'.format(self.app_database)]

        print(flask_database)

        collections = flask_database.getCollectionNames()

        print(collections)



di = DataAggregatorMongo()

di.connect_to_mongo()

di.get_list_of_collections_starting_with("test")