import os
import sys
import sqlite3
from icecream import ic
from collections import namedtuple

from tools import (
    create_tables_indexes, fill_directory_catalog_items, read_csv_to_raw_table,
    insert_root_record_to_catalog, transfer_raw_data_to_catalog, transfer_raw_data_to_quotes,
    transfer_raw_data_to_catalog, transfer_raw_data_to_materials, transfer_raw_data_to_machines,
    transfer_raw_data_to_equipments, transfer_raw_quotes_to_catalog
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

    period = 67
    catalog_data = os.path.join(places[now].data_path, "TABLES_67.csv")
    quotes_data = os.path.join(places[now].data_path, "WORK_PROCESS_67.csv")
    materials_data = os.path.join(places[now].data_path, "1_глава_67_доп.csv")
    machines_data = os.path.join(places[now].data_path, "2_глава_67_доп.csv")
    equipments_data = os.path.join(places[now].data_path, "13_глава_34_доп.csv")

    # period = 68
    # catalog_data = os.path.join(places[now].data_path, "TABLES_68.csv")
    # quotes_data = os.path.join(places[now].data_path, "WORK_PROCESS_68.csv")
    # materials_data = os.path.join(places[now].data_path, "1_глава_68_доп.csv")
    # machines_data = os.path.join(places[now].data_path, "2_глава_68_доп.csv")
    # equipments_data = os.path.join(places[now].data_path, "13_глава_35_доп.csv")

    ic(version, db_name, period)

    # удаляем файл БД если такой есть
    if os.path.isfile(db_name):
        os.unlink(db_name)

    # создать таблицы, индексы, триггеры
    create_tables_indexes(db_name)
    # заполнить данными справочник элементов каталога
    fill_directory_catalog_items(db_name)
    # вставить корневую запись в каталог
    insert_root_record_to_catalog(db_name)

    # --- > Расценки
    # --------------------- > Каталог
    read_csv_to_raw_table(db_name, catalog_data, period)
    transfer_raw_quotes_to_catalog(db_name)
    # # ----------------------- > Данные
    # ic(quotes_data)
    # read_csv_to_raw_table(db_name, quotes_data, period)
    # transfer_raw_data_to_quotes(db_name)
    #
    # # --- > Материалы Глава 1
    # # --------------------- > Каталог Материалы
    # ic(materials_data)
    # read_csv_to_raw_table(db_name, materials_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='materials', main_code='1')
    # # ----------------------- > Данные Материалы
    # transfer_raw_data_to_materials(db_name)
    #
    # # --- > Машины Глава 2
    # # ---------------------- > Каталог Машины
    # ic(machines_data)
    # read_csv_to_raw_table(db_name, machines_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='machines', main_code='2')
    # # ----------------------- > Данные Машины
    # transfer_raw_data_to_machines(db_name)
    # #
    # # # --- > Оборудование Глава 13
    # # # ---------------------- > Каталог
    # # ic(equipments_data)
    # read_csv_to_raw_table(db_name, equipments_data, 67)
    # transfer_raw_data_to_catalog(db_name, directory='equipments', main_code='13')
    # transfer_raw_data_to_equipments(db_name)


