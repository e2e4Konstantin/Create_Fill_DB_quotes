import os
import sqlite3
from icecream import ic

from tools import create_tables_indexes, insert_root_record_to_catalog, fill_directory_catalog_items, fill_catalog

if __name__ == '__main__':
    version = f"{sqlite3.version} {sqlite3.sqlite_version}"

    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    # data_path = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\csv"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")
    catalog_data = os.path.join(data_path, "TABLES_67.csv")

    ic(db_name)
    ic(catalog_data)
    ic(version)

    # удаляем файл БД если такой есть
    if os.path.isfile(db_name):
        os.unlink(db_name)

    # создать таблицы, индексы, триггеры
    create_tables_indexes(db_name)
    # заполнить данными справочник элементов каталога
    fill_directory_catalog_items(db_name)
    # вставить корневую запись в каталог
    insert_root_record_to_catalog(db_name)
    # заполнить данными каталог
    fill_catalog(db_name, catalog_data, period=67)


