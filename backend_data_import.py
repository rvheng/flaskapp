from data_import_scripts import data_import

di = data_import.DataImport()

di.import_csvs("asm")

di.import_csvs("gasoline_prices")