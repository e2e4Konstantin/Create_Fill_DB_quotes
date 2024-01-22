import sqlite3
from icecream import ic

from config import dbTolls, teams
from sql_queries import sql_raw_queries, sql_origins, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value
from tools.shared_features import (
    get_directory_id, get_product_by_code, update_product, insert_product,
    delete_last_period_product_row, get_origin_id, get_origin_row_by_id, get_catalog_id_by_origin_code,
    transfer_raw_items
)


def _make_data_from_raw_quote(db: dbTolls, origin_id: int, raw_quote: sqlite3.Row, item_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированной расценкой и id типа записи.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    raw_period = get_integer_value(raw_quote["PERIOD"])
    holder_code = raw_quote['GROUP_WORK_PROCESS']
    if holder_code is None:
        catalog_name = get_origin_row_by_id(db, origin_id)["name"]
        # указатель на корневую запись каталога
        holder_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=catalog_name)
    else:
        # в Каталоге!!! ищем родительскую запись с шифром holder_cod
        holder_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=clear_code(holder_code))
    if holder_id:
        code = clear_code(raw_quote["PRESSMARK"])
        description = text_cleaning(raw_quote["TITLE"]).capitalize()
        measurer = text_cleaning(raw_quote["UNIT_OF_MEASURE"])
        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
        # period, code, description, measurer, full_code
        data = (holder_id, item_id, origin_id, raw_period, code, description, measurer, None)
        return data
    else:
        output_message_exit(
            f"Для расценки {raw_quote['PRESSMARK']!r} в Каталоге не найдена родительская запись",
            f"шифр {holder_code!r}"
        )
    return None


def _get_raw_quotes(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все записи расценок из сырой таблицы. """
    results = db.go_select(sql_raw_queries["select_rwd_all"])
    if not results:
        output_message_exit(f"в RAW таблице с Расценками нет записей:", f"tblRawData пустая")
        return None
    return results


def transfer_raw_data_to_quotes(db_filename: str, catalog_name: str):
    """ Записывает расценки из сырой таблицы tblRawData в рабочую таблицу tblProducts. """

    with dbTolls(db_filename) as db:
        ic("\n")
        ic("Заполняем данные по Расценкам:")
        # прочитать исходные данные по расценкам
        raw_quotes = _get_raw_quotes(db)
        if raw_quotes is None:
            return None
        directory, item_name = "units", "quote"
        transfer_raw_items(db, catalog_name, directory, item_name, _make_data_from_raw_quote, raw_quotes)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    ic(db_name)

    transfer_raw_data_to_quotes(db_name)
