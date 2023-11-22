import os
import sqlite3
from icecream import ic

from sql_queries import sql_catalog
from config import dbTolls


def create_tables_indexes(db_file_name: str):
    # удаляем файл если такой есть
    if os.path.isfile(db_file_name):
        os.unlink(db_file_name)
    with dbTolls(db_file_name) as db:
        # -- tblDirectoryItems --
        db.go_execute(sql_catalog["delete_table_directory"])
        db.go_execute(sql_catalog["delete_index_directory"])

        db.go_execute(sql_catalog["create_table_directory_items"])
        db.go_execute(sql_catalog["create_index_director_items"])

        # -ryDirectoryItems --
        db.go_execute(sql_catalog["delete_table_history_directory"])
        db.go_execute(sql_catalog["delete_index_history_directory"])
        db.go_execute(sql_catalog["delete_trigger_history_directory_items_insert"])
        db.go_execute(sql_catalog["delete_trigger_history_directory_items_delete"])
        db.go_execute(sql_catalog["delete_trigger_history_directory_items_update"])

        db.go_execute(sql_catalog["create_table_history_directory_items"])
        db.go_execute(sql_catalog["create_index_history_directory_items"])

        db.go_execute(sql_catalog["create_trigger_history_directory_items_insert"])
        db.go_execute(sql_catalog["create_trigger_history_directory_items_delete"])
        db.go_execute(sql_catalog["create_trigger_history_directory_items_update"])


if __name__ == '__main__':
    version = f"{sqlite3.version} {sqlite3.sqlite_version}"
    db_name = os.path.join(r"F:\Kazak\GoogleDrive\Python_projects\DB", "quotes_test.sqlite3")
    ic(version)
    ic(db_name)
    create_tables_indexes(db_name)

    with dbTolls(db_name) as db:

        conn.execute(query_root_ins, src_catalog_items[0])
        for data in src_catalog_items[1:]:
            ic(data)
            conn.execute(query_ins, data)