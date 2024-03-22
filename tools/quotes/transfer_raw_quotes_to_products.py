import sqlite3
from icecream import ic

from config import dbTolls, teams, MAIN_RECORD_CODE
from sql_queries import sql_raw_queries, sql_origins, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import clear_code, text_cleaning, get_integer_value
from tools.shared.shared_features import (
    get_directory_id, get_product_by_code, update_product, insert_product,
    delete_last_period_product_row, get_origin_id, get_origin_row_by_id, get_catalog_id_by_origin_code,
    transfer_raw_items
)


def _make_data_from_raw_quote(
    db: dbTolls, origin_id: int, raw_quote: sqlite3.Row, item_id: int, period_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с загруженной расценкой и id типа записи.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    holder_code = raw_quote['gwp_pressmark']

    if holder_code is None:
        # указатель на корневую запись каталога
        main_catalog_code = MAIN_RECORD_CODE
        holder_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=main_catalog_code)
    else:
        # в Каталоге!!! ищем родительскую запись с шифром holder_cod
        holder_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=clear_code(holder_code))
    if holder_id:
        code = clear_code(raw_quote["pressmark"])
        description = text_cleaning(raw_quote["title"]).capitalize()
        measurer = text_cleaning(raw_quote["unit_measure "])

        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
        # period, code, description, measurer, full_code
        data = (holder_id, item_id, origin_id, period_id,
                code, description, measurer, None)
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


def transfer_raw_quotes_to_products(db_file: str, catalog_name: str, period_id: int):
    """ Заполняет в таблицу tblProducts расценками из RAW таблицы tblRawData.
    """
    with dbTolls(db_file) as db:
        ic("\n")
        ic("Заполняем данные по Расценкам:")
        raw_quotes = _get_raw_quotes(db)
        if raw_quotes is None:
            return None
        directory, item_name = "units", "quote"
        transfer_raw_items(db, catalog_name, directory, item_name, _make_data_from_raw_quote, raw_quotes, period_id)


if __name__ == '__main__':

    transfer_raw_quotes_to_products(db_file, TON_ORIGIN, period_id)


