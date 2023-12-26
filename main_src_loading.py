import os
import sys
import sqlite3
from icecream import ic
from collections import namedtuple

from tools import (
    create_tables_indexes, fill_directory_catalog_items, read_csv_to_raw_table,
    insert_root_record_to_catalog, transfer_raw_table_data_to_catalog, transfer_raw_data_to_quotes,
    transfer_raw_data_to_catalog
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
now = "office"  # "office"  # "home"

if __name__ == '__main__':
    version = f"SQLite: {sqlite3.sqlite_version}\nPython: {sys.version}"
    db_name = os.path.join(places[now].db_path, db_name)

    # period = 67
    # catalog_data = os.path.join(places[now].data_path, "TABLES_67.csv")
    # quotes_data = os.path.join(places[now].data_path, "WORK_PROCESS_67.csv")

    period = 68
    catalog_data = os.path.join(places[now].data_path, "TABLES_68.csv")
    quotes_data = os.path.join(places[now].data_path, "WORK_PROCESS_68.csv")
    materials_data = os.path.join(places[now].data_path, "1_глава_68_доп.csv")
    machines_data = os.path.join(places[now].data_path, "2_глава_68_доп.csv")
    equipments_data = os.path.join(places[now].data_path, "13_глава_35_доп.csv")

    ic(version, db_name, period)

    # # удаляем файл БД если такой есть
    # if os.path.isfile(db_name):
    #     os.unlink(db_name)
    #
    # # создать таблицы, индексы, триггеры
    # create_tables_indexes(db_name)
    # # заполнить данными справочник элементов каталога
    # fill_directory_catalog_items(db_name)
    # # вставить корневую запись в каталог
    # insert_root_record_to_catalog(db_name)

    # --- > Расценки Каталог
    # прочитать из csv файла данные для Каталога в таблицу tblRawData для периода period
    # read_csv_to_raw_table(db_name, catalog_data, period)
    # заполнить Каталог данными из таблицы tblRawData
    transfer_raw_table_data_to_catalog(db_name)
    # #
    # # # --- > 2 Расценки
    # # # прочитать из csv файла данные для Расценок в таблицу tblRawData для периода period
    # # ic(quotes_data)
    # # read_csv_to_raw_table(db_name, quotes_data, period)
    # # # заполнить Расценки данными из таблицы tblRawData
    # # transfer_raw_data_to_quotes(db_name)
    #

    # # --- > Материалы Глава 1
    # ic(materials_data)
    # # прочитать из csv файла данные для Машин в таблицу tblRawData для периода period
    # read_csv_to_raw_table(db_name, materials_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='materials', main_code='1')
    # # заполнить Материалы данными из таблицы tblRawData
    # # transfer_raw_data_to_catalog_materials(db_name)
    # # transfer_raw_data_to_materials(db_name)
    #
    # # --- > Машины Глава 2
    # ic(machines_data)
    # read_csv_to_raw_table(db_name, machines_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='machines', main_code='2')
    #
    # # --- > Оборудование Глава 13
    # ic(equipments_data)
    # read_csv_to_raw_table(db_name, equipments_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='equipments', main_code='13')

    # заполнить Машины данными из таблицы tblRawData
    # machines_transfer_raw_data_to_catalog(db_name)
    # transfer_raw_data_to_materials(db_name)
