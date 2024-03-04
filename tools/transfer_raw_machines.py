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


def _make_data_from_raw_machine(db: dbTolls, origin_id: int, raw_machine: sqlite3.Row, item_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированной машиной и id типа записи для материала.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу tblProducts.
    """
    # получить указатель на родительскую запись каталога
    holder_id = get_parent_catalog_id(db, origin_id=origin_id, raw_parent_id=raw_machine['PARENT'])
    if holder_id is None:
        return None
    raw_code = clear_code(raw_machine['CMT'])
    description = text_cleaning(raw_machine['TITLE']).capitalize()
    measurer = text_cleaning(raw_machine['Ед.Изм.'])
    raw_period = get_integer_value(raw_machine["PERIOD"])
    # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
    # period, code, description, measurer, full_code
    data = (holder_id, item_id, origin_id, raw_period, raw_code, description, measurer, None)
    return data


def _get_raw_data_machines(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все Машины из сырой таблицы без элементов каталога. """
    raw_machines = db.go_select(sql_raw_queries["select_rwd_machines"])
    if not raw_machines:
        output_message_exit(f"в RAW таблице с Машинами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_machines


def transfer_raw_data_to_machines(db_filename: str, catalog_name: str):
    """ Записывает МАШИНЫ из сырой таблицы tblRawData в рабочую таблицу tblProducts. """
    with dbTolls(db_filename) as db:
        # прочитать исходные данные по машинам
        raw_machines = _get_raw_data_machines(db)
        if raw_machines is None:
            return None
        directory, item_name = "units", "machine"
        transfer_raw_items(db, catalog_name, directory, item_name, _make_data_from_raw_machine, raw_machines)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    ic(db_name)

    transfer_raw_data_to_machines(db_name)
