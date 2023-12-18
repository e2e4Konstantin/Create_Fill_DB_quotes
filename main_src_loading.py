import os
import sqlite3
from icecream import ic
from collections import namedtuple

from tools import (
    create_tables_indexes, fill_directory_catalog_items, read_csv_to_raw_table, insert_root_record_to_catalog
)

PlacePath = namedtuple("PlacePath", ["data_path", "db_path"])

places = {
    "office": PlacePath(
        data_path=r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\csv",
        db_path=r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    ),
    "home": PlacePath(
        data_path=r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv",
        db_path=r"F:\Kazak\GoogleDrive\Python_projects\DB"
    ),
}
db_name = "Normative.sqlite3"
now = "home"  # "office"  # "home"

if __name__ == '__main__':
    version = f"{sqlite3.version} {sqlite3.sqlite_version}"
    db_name = os.path.join(places[now].db_path, db_name)

    period = 68
    catalog_data = os.path.join(places[now].data_path, "TABLES_68.csv")

    ic(version)
    ic(db_name)
    ic(catalog_data)

    # удаляем файл БД если такой есть
    if os.path.isfile(db_name):
        os.unlink(db_name)

    # создать таблицы, индексы, триггеры
    create_tables_indexes(db_name)
    # заполнить данными справочник элементов каталога
    fill_directory_catalog_items(db_name)
    # вставить корневую запись в каталог
    insert_root_record_to_catalog(db_name)

    # 1 Каталог
    # прочитать из csv файла данные для Каталога в таблицу tblRawData для периода period
    # read_csv_to_raw_table(db_name, catalog_data, period)

    # # заполнить Каталог данными из таблицы tblRawData
    # transfer_raw_table_data_to_catalog(db_name)
