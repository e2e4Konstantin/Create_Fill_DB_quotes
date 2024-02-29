import os
import sys
import sqlite3
from icecream import ic
from collections import namedtuple


from config import work_place, TON_CATALOG, PNWC_CATALOG

from tools import (
    create_tables_indexes, fill_directory_origins, fill_directory_catalog_items, insert_root_record_to_catalog,
    read_csv_to_raw_table, transfer_raw_quotes_to_catalog, transfer_raw_data_to_quotes,
    transfer_raw_data_to_catalog, transfer_raw_data_to_materials, create_index_resources_raw_data,
    transfer_raw_data_to_machines, transfer_raw_data_to_equipments, transfer_raw_pnwc_resources_to_catalog,
    transfer_raw_data_to_pnwc_resources, delete_raw_tables
)


def _create_new_db(db_file_name: str):
    if os.path.isfile(db_file_name):
        os.unlink(db_file_name)  # удаляем файл БД если такой есть
    create_tables_indexes(db_file_name)  # создать таблицы, индексы, триггеры
    fill_directory_origins(db_file_name)  # заполнить справочник происхождения tblOrigins
    fill_directory_catalog_items(db_file_name)  # заполнить данными справочник элементов каталога
    insert_root_record_to_catalog(
        db_file_name, catalog=TON_CATALOG, code=TON_CATALOG, period=0, description='Справочник нормативов ТСН'
    )  # вставить корневую запись для ТСН
    insert_root_record_to_catalog(
        db_file_name, catalog=PNWC_CATALOG, code=PNWC_CATALOG, period=0, description='Справочник ресурсов НЦКР'
    )  # вставить корневую запись в каталог для НЦКР


if __name__ == '__main__':
    version = f"SQLite: {sqlite3.sqlite_version}\nPython: {sys.version}"
    now = "home"  # office  # home
    ic(now)
    db_name, data_path, param_path = work_place(now)

    # period = 67
    # catalog_data = os.path.join(data_path, "TABLES_67.csv")
    # quotes_data = os.path.join(data_path, "WORK_PROCESS_67.csv")
    # materials_data = os.path.join(data_path, "1_глава_67_доп.csv")
    # machines_data = os.path.join(data_path, "2_глава_67_доп.csv")
    # equipments_data = os.path.join(data_path, "13_глава_34_доп.csv")
    # pnwc_catalog = os.path.join(data_path, "Каталог_НЦКР_Временный_каталог_Март_2022_Ресурсы_ТСН.csv")
    # pnwc_resource = os.path.join(data_path, "Данные_НЦКР_Временный_каталог_НЦКР_2023_4_кв.csv")

    period = 68
    catalog_data = os.path.join(data_path, "TABLES_68.csv")
    quotes_data = os.path.join(data_path, "WORK_PROCESS_68.csv")
    materials_data = os.path.join(data_path, "1_глава_68_доп.csv")
    machines_data = os.path.join(data_path, "2_глава_68_доп.csv")
    equipments_data = os.path.join(data_path, "13_глава_35_доп.csv")

    ic(version, db_name, period)
    _create_new_db(db_name)

    # --- > Расценки
    # --------------------- > Каталог
    ic(catalog_data)
    read_csv_to_raw_table(db_name, catalog_data, period)
    transfer_raw_quotes_to_catalog(db_name, catalog_name=TON_CATALOG)
    # ---------------------- > Данные
    ic(quotes_data)
    read_csv_to_raw_table(db_name, quotes_data, period)
    transfer_raw_data_to_quotes(db_name, catalog_name=TON_CATALOG)
    #
    # --- > Материалы Глава 1
    # --------------------- > Каталог Материалы
    ic(materials_data)
    read_csv_to_raw_table(db_name, materials_data, period)
    transfer_raw_data_to_catalog(db_name, directory='materials', catalog_name=TON_CATALOG, main_code='1')
    # ----------------------- > Данные Материалы
    create_index_resources_raw_data(db_name)
    transfer_raw_data_to_materials(db_name, catalog_name=TON_CATALOG)
    #
    # --- > Машины Глава 2
    # ---------------------- > Каталог Машины
    ic(machines_data)
    read_csv_to_raw_table(db_name, machines_data, period)
    transfer_raw_data_to_catalog(db_name, directory='machines', catalog_name=TON_CATALOG, main_code='2')
    # ----------------------- > Данные Машины
    transfer_raw_data_to_machines(db_name, catalog_name=TON_CATALOG)
    #
    # --- > Оборудование Глава 13
    # ---------------------- > Каталог
    ic(equipments_data)
    read_csv_to_raw_table(db_name, equipments_data, period)
    transfer_raw_data_to_catalog(db_name, directory='equipments', catalog_name=TON_CATALOG, main_code='13')
    # ----------------------- > Данные Оборудование
    transfer_raw_data_to_equipments(db_name, catalog_name=TON_CATALOG)

    # # --- > Ресурсы НЦКР
    # # --------------------- > Каталог НЦКР
    # ic(pnwc_catalog)
    # period = 55
    # read_csv_to_raw_table(db_name, pnwc_catalog, period)
    # transfer_raw_pnwc_resources_to_catalog(db_name, catalog_name=PNWC_CATALOG)
    # # ----------------------- > Данные Ресурсы НЦКР
    # read_csv_to_raw_table(db_name, pnwc_resource, period, new_column_name=['N', 'NPP', 'Шифр новый действующий', 'Уточненное наименование по данным мониторинга'])
    # transfer_raw_data_to_pnwc_resources(db_name, catalog_name=PNWC_CATALOG)

    delete_raw_tables(db_name)

    #
