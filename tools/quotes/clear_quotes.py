import sqlite3
from icecream import ic
from config import (
    dbTolls,
    LocalData,
    DirectoryItem,
    MAIN_RECORD_CODE,
    TON_ORIGIN,
)

from sql_queries import sql_items_queries
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import clear_code, title_catalog_extraction, code_to_number
from tools.shared.shared_features import (
    get_sorted_directory_items,
    get_catalog_id_by_origin_code,
    get_catalog_row_by_code,
    delete_catalog_old_period_for_parent_code,
    get_raw_data_items,
    update_catalog,
    insert_raw_catalog,
    get_origin_id,
    get_period_by_id,
)



def delete_quotes_catalog(db_file_name: str, catalog_name: str, period_id: int):
    with dbTolls(db_file_name) as db:
        # получить идентификатор каталога
        origin_id = get_origin_id(db, origin_name=catalog_name)
        ic(origin_id)


if __name__ == "__main__":
    ...
    # location = "office"  # office  # home
    # data_paths: LocalData = get_data_location(location)
    # (data_paths.db_file,)
    # catalog_name = (TON_ORIGIN,)
    # period_id = 67

    # transfer_raw_quotes_to_catalog(
    #     data_paths.db_file, catalog_name=TON_ORIGIN, period_id=period_id
    # )
