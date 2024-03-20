from icecream import ic

from config import LocalData
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table



def parsing_raw_quotes(data_paths: LocalData) -> int:
    """заполнить каталог расценок и сами расценки"""
    file, period = data_paths.get_data('catalog_67_dop')
    load_csv_to_raw_table(csv_file=file, db_file=data_paths.db_file, delimiter = ";")
    return 0

    # period = 67
    # catalog_data = os.path.join(data_path, "TABLES_67.csv")
    # quotes_data = os.path.join(data_path, "WORK_PROCESS_67.csv")

    # period = 68
    # catalog_data = os.path.join(data_path, "TABLES_68.csv")
    # quotes_data = os.path.join(data_path, "WORK_PROCESS_68.csv")

    # read_csv_to_raw_table(db_name, catalog_data, period)
    # transfer_raw_quotes_to_catalog(db_name, catalog_name=TON_CATALOG)
    # # ---------------------- > Данные
    # ic(quotes_data)
    # read_csv_to_raw_table(db_name, quotes_data, period)
    # transfer_raw_data_to_quotes(db_name, catalog_name=TON_CATALOG)

if __name__ == "__main__":
    from data_path import set_data_location

    location = "home"  # office
    di = set_data_location(location)
    r = parsing_raw_quotes(di)
