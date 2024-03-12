import sqlite3
import os
from icecream import ic
from sql_catalog import sql_catalog

from config import src_catalog_items

if __name__ == '__main__':
    version = f"{sqlite3.version} {sqlite3.sqlite_version}"
    db_name = os.path.join(r"F:\Kazak\GoogleDrive\Python_projects\DB", "quotes_test.sqlite3")
    ic(version)
    ic(db_name)
    # удаляем файл если такой есть
    if os.path.isfile(db_name):
        os.unlink(db_name)

    query_db = sql_catalog["create_table_directory"]
    query_idx = sql_catalog["create_index_director_items"]
    query_root_ins = sql_catalog["insert_root_item_directory"]
    query_ins = sql_catalog["insert_item_directory"]


    # for data in src_catalog_items[1:]:
    #     ic(data)

    with sqlite3.connect(db_name, isolation_level=None) as conn:
        conn.execute("DROP TABLE IF EXISTS tblDirectoryItems;")
        conn.execute("DROP INDEX IF EXISTS idxDirectoryCodes;")
        conn.execute(query_db)
        conn.execute(query_idx)
        conn.execute(query_root_ins, src_catalog_items[0])
        for data in src_catalog_items[1:]:
            ic(data)
            conn.execute(query_ins, data)

