import sqlite3
from icecream import ic

from config import dbTolls

from files_features import output_message_exit
from tools.shared.code_tolls import clear_code, text_cleaning, get_integer_value
from sql_queries import sql_raw_queries

from tools.shared.shared_features import (
    update_product, insert_product, get_parent_catalog_id,
    get_product_by_code, delete_last_period_product_row, get_directory_id, get_origin_id, transfer_raw_items
)


def _make_data_from_raw_equipment(db: dbTolls, origin_id: int, raw_equipment: sqlite3.Row, equipment_item_id: int) -> tuple | None:

    """ Получает строку из таблицы tblRawData с оборудованием и id типа записи для оборудования.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу tblProducts.
    """
    holder_id = get_parent_catalog_id(db, origin_id=origin_id, raw_parent_id=raw_equipment['PARENT'])
    if holder_id is None:
        return None
    raw_code = clear_code(raw_equipment['CMT'])
    description = text_cleaning(raw_equipment['TITLE']).capitalize()
    measurer = text_cleaning(raw_equipment['Ед.Изм.'])
    raw_period = get_integer_value(raw_equipment["PERIOD"])
    # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
    # period, code, description, measurer, full_code
    data = (holder_id, equipment_item_id, origin_id, raw_period, raw_code, description, measurer, None)
    return data


def _get_raw_data_equipments(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все Оборудования из сырой таблицы без элементов каталога. """
    raw_equipments = db.go_select(sql_raw_queries["select_rwd_equipments"])
    if not raw_equipments:
        output_message_exit(f"в RAW таблице с Машинами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_equipments


def transfer_raw_data_to_equipments(db_filename: str, catalog_name: str):
    """ Записывает Оборудование из сырой таблицы tblRawData в рабочую таблицу tblProducts. """

    with dbTolls(db_filename) as db:
        # прочитать исходные данные по машинам
        raw_equipments = _get_raw_data_equipments(db)
        if raw_equipments is None:
            return None
        directory, item_name = "units", "equipment"
        transfer_raw_items(db, catalog_name, directory, item_name, _make_data_from_raw_equipment, raw_equipments)




if __name__ == '__main__':
    import os
    from tools import read_csv_to_raw_table, transfer_raw_data_to_catalog

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
    equipments_data = os.path.join(data_path, "13_глава_34_доп.csv")
    ic(db_name)

    read_csv_to_raw_table(db_name, equipments_data, 67)
    transfer_raw_data_to_catalog(db_name, directory='equipments', main_code='13')

    transfer_raw_data_to_equipments(db_name)
