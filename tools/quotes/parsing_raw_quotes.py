
from config import dbTolls, LocalData, get_data_location, TON_ORIGIN
from icecream import ic

from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from files_features import create_abspath_file
from tools.quotes.transfer_raw_catalog_quotes import transfer_raw_quotes_to_catalog



# заполнить каталог расценок и сами расценки
def parsing_quotes(location: str) -> int:
    # найти период
    # загрузить каталог для периода
    # загрузить расценки для периода

    data_paths: LocalData = get_data_location(location)
    data_paths.periods_data.sort(key=lambda x: x['supplement'])


    period_number = 0
    catalog_csv_file = create_abspath_file(
        data_paths.quote_catalog_path,
        data_paths.periods_data[period_number]['quotes_catalog_csv_file']
        )

    load_csv_to_raw_table(
        csv_file=catalog_csv_file, db_file=data_paths.db_file, delimiter=","
        )

    period_id = data_paths.periods_data[period_number]["id"]

    transfer_raw_quotes_to_catalog(
        data_paths.db_file, catalog_name=TON_ORIGIN, period_id=period_id
        )

    return 0


if __name__ == "__main__":
    location = "office"

    parsing_quotes(location)



# # --- > Расценки
# # --------------------- > Каталог
# ic(catalog_data)
# read_csv_to_raw_table(db_name, catalog_data, period)
# transfer_raw_quotes_to_catalog(db_name, catalog_name=TON_CATALOG)
# # ---------------------- > Данные
# ic(quotes_data)
# read_csv_to_raw_table(db_name, quotes_data, period)
# transfer_raw_data_to_quotes(db_name, catalog_name=TON_CATALOG)
