from data_import_scripts import data_import_mongo
import time

start = time.time()

di = data_import_mongo.DataImportMongo()

di.import_csvs("asm")

di.import_csvs("gasoline_prices")

end = time.time()

print(end - start)
