import sqlite3
from icecream import ic

from config import dbTolls, teams
from sql_queries import sql_raw_queries, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import clear_code, text_cleaning, get_integer_value

from tools.shared.shared_features import (
    get_parent_catalog_id, update_product, insert_product,
    get_product_by_code, delete_last_period_product_row, get_directory_id, get_origin_id, transfer_raw_items
)


def _make_data_from_raw_material(db: dbTolls, origin_id: int, raw_material: sqlite3.Row, item_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированным материалом и id типа записи для материала.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    # получить указатель на родительскую запись каталога
    holder_id = get_parent_catalog_id(db, origin_id=origin_id, raw_parent_id=raw_material['PARENT'])
    if holder_id is None:
        return None
    raw_code = clear_code(raw_material['CMT'])
    description = text_cleaning(raw_material['TITLE']).capitalize()
    measurer = text_cleaning(raw_material['Ед.Изм.'])
    raw_period = get_integer_value(raw_material["PERIOD"])
    # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
    # period, code, description, measurer, full_code
    data = (holder_id, item_id, origin_id, raw_period, raw_code, description, measurer, None)
    return data


def _get_raw_data_materials(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все Материалы из сырой таблицы без элементов каталога. """
    raw_materials = db.go_select(sql_raw_queries["select_rwd_materials"])
    if not raw_materials:
        output_message_exit(f"в RAW таблице с Материалами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_materials


def transfer_raw_data_to_materials(db_filename: str, catalog_name: str):
    """ Записывает МАТЕРИАЛЫ из сырой таблицы tblRawData в рабочую таблицу tblProducts. """
    with dbTolls(db_filename) as db:
        # прочитать исходные данные по машинам
        raw_materials = _get_raw_data_materials(db)
        if raw_materials is None:
            return None
        directory, item_name = "units", "material"
        transfer_raw_items(db, catalog_name, directory, item_name, _make_data_from_raw_material, raw_materials)



if __name__ == '__main__':
    import os
    from tools import read_csv_to_raw_table, transfer_raw_data_to_catalog

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
    materials_data = os.path.join(data_path, "1_глава_67_доп.csv")
    ic(db_name)

    # read_csv_to_raw_table(db_name, materials_data, 67)
    # transfer_raw_data_to_catalog(db_name, directory='materials', main_code='1')
    transfer_raw_data_to_materials(db_name)
