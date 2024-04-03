
from config import LocalData, TON_ORIGIN
from icecream import ic

from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from files_features import create_abspath_file
from tools.quotes.transfer_raw_catalog_quotes import transfer_raw_quotes_to_catalog
from tools.quotes.transfer_raw_quotes_to_products import transfer_raw_quotes_to_products




# заполнить каталог расценок и сами расценки
def parsing_quotes(location: LocalData) -> int:
    """
    Заполняет Таблицы БД (tblCatalogs, tblProducts) каталог/дерево
    расценок и сами расценки. Читает данные из CSV файлов в tblRawData.
    Переносит данные из tblRawData в боевые таблицы.
    """
    catalog_path: str = location.quote_catalog_path
    quote_path: str = location.quote_data_path
    db_file: str = location.db_file
    print()
    ic("===>>> Загружаем расценки.")
    for period in location.periods_data:   #[:2]:
        print()
        ic(period["title"])
        period_id = period["id"]
        # грузим каталог
        catalog_csv_file = create_abspath_file(catalog_path, period['quotes_catalog_csv_file'])
        load_csv_to_raw_table(catalog_csv_file, db_file, delimiter=",")
        ic("==>> Загружаем каталог для расценок:")
        transfer_raw_quotes_to_catalog(db_file, TON_ORIGIN, period_id)

        # грузим расценки
        quote_csv_file = create_abspath_file(quote_path, period['quotes_data_csv_file'])
        load_csv_to_raw_table(quote_csv_file, db_file, delimiter=",")
        ic("==>> Загружаем расценки:")
        transfer_raw_quotes_to_products(db_file, TON_ORIGIN, period_id)
    return 0


if __name__ == "__main__":

    location = LocalData("office") # office  # home
    parsing_quotes(location)


