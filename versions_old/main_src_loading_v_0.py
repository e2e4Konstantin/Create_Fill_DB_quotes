# import os
# import sqlite3
# from icecream import ic
#
# # from tools import (
# #     create_tables_indexes, insert_root_record_to_catalog, fill_directory_catalog_items,
# #     read_csv_to_raw_table, transfer_raw_table_data_to_catalog, transfer_raw_table_data_to_quotes
# # )
#
# if __name__ == '__main__':
#     version = f"{sqlite3.version} {sqlite3.sqlite_version}"
#
#     data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
#     db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
#
#     # data_path = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\csv"
#     # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
#
#     db_name = os.path.join(db_path, "quotes_test.sqlite3")
#
#     # period = 67
#     # catalog_data = os.path.join(data_path, "TABLES_67.csv")
#     # quotes_data = os.path.join(data_path, "WORK_PROCESS_67.csv")
#
#     period = 68
#     catalog_data = os.path.join(data_path, "TABLES_68.csv")
#     quotes_data = os.path.join(data_path, "WORK_PROCESS_68.csv")
#     resources_data =
#
#     ic(version)
#     ic(db_name)
#
#     # # удаляем файл БД если такой есть
#     # if os.path.isfile(db_name):
#     #     os.unlink(db_name)
#     #
#     # # создать таблицы, индексы, триггеры
#     # create_tables_indexes(db_name)
#     # # # заполнить данными справочник элементов каталога
#     # # fill_directory_catalog_items(db_name)
#     # # # вставить корневую запись в каталог
#     # # insert_root_record_to_catalog(db_name)
#     #
#     # # ----------------------------------------------
#     #
#     # # 1 Каталог
#     # # прочитать из csv файла данные для Каталога в таблицу tblRawData для периода period
#     # ic(catalog_data)
#     # read_csv_to_raw_table(db_name, catalog_data, period)
#     # # заполнить Каталог данными из таблицы tblRawData
#     # transfer_raw_table_data_to_catalog(db_name)
#     #
#     # # 2 Расценки
#     # # прочитать из csv файла данные для Расценок в таблицу tblRawData для периода period
#     # ic(quotes_data)
#     # read_csv_to_raw_table(db_name, quotes_data, period)
#     # # заполнить Расценки данными из таблицы tblRawData
#     # transfer_raw_table_data_to_quotes(db_name)
#     #
#     # # 3 Ресурсы
#     # ic(resources_data)
#     # read_csv_to_raw_table(db_name, resources_data, period)
#
#
#
#
#     # raw_table_name = sql_raw_data["table_name_raw_data"]
#     # operating_db.go_execute(f"DROP TABLE IF EXISTS {table_name};")
