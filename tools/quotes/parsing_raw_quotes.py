
from config import LocalData, TON_ORIGIN
from icecream import ic

from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from files_features import create_abspath_file
from tools.quotes.transfer_raw_catalog_quotes import transfer_raw_quotes_to_catalog
from tools.quotes.transfer_raw_quotes_to_products import transfer_raw_quotes_to_products


# {
#     "basic_id": 150719989,
#     "id": 66,
#     "index": 198,
#     "quotes_catalog_csv_file": "Catalog_Дополнение_68_id_150719989.csv",
#     "quotes_data_csv_file": "Quotes_Дополнение_68_id_150719989.csv",
#     "supplement": 68,
#     "title": "Дополнение 68"
# },




# заполнить каталог расценок и сами расценки
def parsing_quotes(data_paths: LocalData) -> int:
    """Читает данные из CSV файлов в tblRawData: дерево расценок и сами расценки.
       Переносит эти данные в боевые таблицы.
    """
    data_paths.periods_data.sort(key=lambda x: x['supplement'])

    catalog_path = data_paths.quote_catalog_path
    quote_path = data_paths.quote_data_path
    db_file = data_paths.db_file

    for period in data_paths.periods_data:   #[:2]:
        ic(period)
        period_id = period["id"]
        # грузим каталог
        catalog_csv_file = create_abspath_file(catalog_path, period['quotes_catalog_csv_file'])
        load_csv_to_raw_table(catalog_csv_file, db_file, delimiter=",")
        transfer_raw_quotes_to_catalog(db_file, TON_ORIGIN, period_id)

        # грузим расценки
        quote_csv_file = create_abspath_file(quote_path, period['quotes_data_csv_file'])
        load_csv_to_raw_table(quote_csv_file, db_file, delimiter=",")

        transfer_raw_quotes_to_products(db_file, TON_ORIGIN, period_id)
    return 0


if __name__ == "__main__":

    location = LocalData("office") # office  # home
    parsing_quotes(location)


