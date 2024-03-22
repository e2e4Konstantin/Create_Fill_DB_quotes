# import os
# import sys
# import sqlite3
from icecream import ic

from tools import db_create_fill_directory, parsing_raw_periods
from tools.quotes.parsing_raw_quotes import parsing_quotes
# from tools import (read_csv_to_raw_table, transfer_raw_quotes_to_catalog, transfer_raw_data_to_quotes,
#     transfer_raw_data_to_catalog, transfer_raw_data_to_materials, create_index_resources_raw_data,
#     transfer_raw_data_to_machines, transfer_raw_data_to_equipments, transfer_raw_pnwc_resources_to_catalog,
#     transfer_raw_data_to_pnwc_resources, delete_raw_tables
# )

from config import dbTolls, LocalData, get_data_location

if __name__ == '__main__':
    # version = f"SQLite: {sqlite3.sqlite_version}\nPython: {sys.version}"
    location = "office" # office  # home
    local_path = get_data_location(location)
    ic(location, local_path.db_file)

    db_create_fill_directory(local_path.db_file)    # Создать таблицы и заполнить справочники
    parsing_raw_periods(local_path)                 # заполнить периоды

    parsing_quotes(local_path)                  # заполнить каталог расценок и сами расценки



    # # --- > Расценки
    # # --------------------- > Каталог
    # ic(catalog_data)
    # read_csv_to_raw_table(db_name, catalog_data, period)
    # transfer_raw_quotes_to_catalog(db_name, catalog_name=TON_CATALOG)
    # # ---------------------- > Данные
    # ic(quotes_data)
    # read_csv_to_raw_table(db_name, quotes_data, period)
    # transfer_raw_data_to_quotes(db_name, catalog_name=TON_CATALOG)
    # #
    # # --- > Материалы Глава 1
    # # --------------------- > Каталог Материалы
    # ic(materials_data)
    # read_csv_to_raw_table(db_name, materials_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='materials', catalog_name=TON_CATALOG, main_code='1')
    # # ----------------------- > Данные Материалы
    # create_index_resources_raw_data(db_name)
    # transfer_raw_data_to_materials(db_name, catalog_name=TON_CATALOG)
    # #
    # # --- > Машины Глава 2
    # # ---------------------- > Каталог Машины
    # ic(machines_data)
    # read_csv_to_raw_table(db_name, machines_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='machines', catalog_name=TON_CATALOG, main_code='2')
    # # ----------------------- > Данные Машины
    # transfer_raw_data_to_machines(db_name, catalog_name=TON_CATALOG)
    # #
    # # --- > Оборудование Глава 13
    # # ---------------------- > Каталог
    # ic(equipments_data)
    # read_csv_to_raw_table(db_name, equipments_data, period)
    # transfer_raw_data_to_catalog(db_name, directory='equipments', catalog_name=TON_CATALOG, main_code='13')
    # # ----------------------- > Данные Оборудование
    # transfer_raw_data_to_equipments(db_name, catalog_name=TON_CATALOG)

    # # # --- > Ресурсы НЦКР
    # # # --------------------- > Каталог НЦКР
    # # ic(pnwc_catalog)
    # # period = 55
    # # read_csv_to_raw_table(db_name, pnwc_catalog, period)
    # # transfer_raw_pnwc_resources_to_catalog(db_name, catalog_name=PNWC_CATALOG)
    # # # ----------------------- > Данные Ресурсы НЦКР
    # # read_csv_to_raw_table(db_name, pnwc_resource, period, new_column_name=['N', 'NPP', 'Шифр новый действующий', 'Уточненное наименование по данным мониторинга'])
    # # transfer_raw_data_to_pnwc_resources(db_name, catalog_name=PNWC_CATALOG)

    # # delete_raw_tables(db_name)

    # #
