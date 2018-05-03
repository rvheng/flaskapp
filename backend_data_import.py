from data_import_scripts import data_import
import time

start = time.time()

di = data_import.DataImport()

di.import_csvs("asm")

di.import_csvs("gasoline_prices")

end = time.time()

print(end - start)