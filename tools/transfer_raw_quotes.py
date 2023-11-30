import sqlite3
import pandas as pd
from icecream import ic
from config import dbTolls, items_catalog
from sql_queries import sql_directory_selects, sql_catalog_insert, sql_raw_data, sql_catalog_select, sql_catalog_insert
from files_features import output_message
from tools.code_tolls import clear_code, title_extraction


def _write_raw_quote(connections: sqlite3.Connection, raw_quotes: sqlite3.Row):
    ic(tuple(raw_quotes))




def transfer_raw_table_data_to_quotes(db_filename: str):
    """ Записывает расценки из сырой базы в рабочую """
    with dbTolls(db_filename) as db:
        result = db.go_execute(sql_raw_data["select_rwd_all"])
        if result:
            raw_quotes = result.fetchall()
            success = []
            for row in raw_quotes:
                _write_raw_quote(db, row)



                # ic(type(row))

                # code = clear_code(row["PRESSMARK"])
                # raw_parent_code = row["PARENT_PRESSMARK"]
                # period = int(row["PERIOD"])
                # if raw_parent_code is None:
                #     id_parent = _get_item_id(period=0, code='0', db=db)
                # else:
                #     parent_code = clear_code(raw_parent_code)
                #     id_parent = _get_item_id(period=period, code=parent_code, db=db)
                # # id типа записи
                # id_items = str(item[0])
                # if id_parent and id_items:
                #     description = title_extraction(row["TITLE"], item[1])
                #     # ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
                #     data = (id_parent, period, code, description, int(item[0]))
                #     # ic(data)
                #     message = f"INSERT tblCatalog {item[1]!r} шифр {code!r} период: {period}"
                #     inserted_id = db.go_insert(sql_catalog_insert["insert_catalog"], data, message)
                #     if inserted_id:
                #         success.append(inserted_id)
                # else:
                #     output_message(f"запись {tuple(row)}", f"не добавлена в БД")
            log = f"добавлено {len(success)} записей."
            ic(log)
        else:
            output_message(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                           f"")


if __name__ == '__main__':
    import os

    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")
    ic(db_name)
    transfer_raw_table_data_to_quotes(db_name)
