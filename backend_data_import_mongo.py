from data_import_scripts import data_import_mongo

di = data_import_mongo.DataImportMongo()

#di.test()

di.import_csvs("asm")

di.import_csvs("gasoline_prices")