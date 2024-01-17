import sqlite3
from icecream import ic

from config import dbTolls, teams
from sql_queries import sql_raw_queries, sql_origins, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value
from tools.shared_features import get_directory_id, get_product_row_by_code, update_product, insert_product, \
    delete_last_period_product_row, get_catalog_id_by_period_code, get_origin_id


def _make_data_from_raw_quote(db: dbTolls, raw_quote: sqlite3.Row, item_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированной расценкой и id типа записи.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    raw_period = get_integer_value(raw_quote["PERIOD"])
    holder_code = raw_quote['GROUP_WORK_PROCESS']
    if holder_code is None:
        # указатель на корневую запись каталога
        holder_id = get_catalog_id_by_period_code(db=db, period=0, code='0000')
    else:
        # в Каталоге!!! ищем родительскую запись с шифром holder_cod
        holder_id = get_catalog_id_by_period_code(db=db, period=raw_period, code=clear_code(holder_code))
    if holder_id:
        code = clear_code(raw_quote["PRESSMARK"])
        description = text_cleaning(raw_quote["TITLE"]).capitalize()
        measurer = text_cleaning(raw_quote["UNIT_OF_MEASURE"])
        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, period, code, description, measurer, full_code
        data = (holder_id, item_id, raw_period, code, description, measurer, "")
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





def transfer_raw_data_to_quotes(db_filename: str):
    """ Записывает расценки из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется расценка с таким же шифром, если такая есть то она обновляется,
        если не найдена то вставляется новая.
     """
    with dbTolls(db_filename) as db:
        # получить все строки raw таблицы
        raw_quotes = _get_raw_quotes(db)
        # получить id типа для расценки
        team, name = "units", "quote"
        quote_item_id = get_directory_id(db, directory_team=team, item_name=name)
        inserted_success, updated_success = [], []

        origin_id = get_origin_id(db, origin_name='ТСН')

        for row in raw_quotes:
            raw_code = clear_code(row["PRESSMARK"])
            raw_period = get_integer_value(row["PERIOD"])
            data_line = _make_data_from_raw_quote(db, row, quote_item_id)
            # Найти расценку с шифром raw_cod в таблице расценок tblProducts
            quote = get_product_row_by_code(db=db, product_code=raw_code)
            if quote:
                if raw_period >= quote['period'] and quote_item_id == quote['FK_tblProducts_tblItems']:
                    count_updated = update_product(db, data_line + (quote['ID_tblProduct'],))
                    if count_updated:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка Обновления Расценки: {raw_code!r} или item_type не совпадает {quote_item_id}",
                        f"период Расценки {quote['period']} старше загружаемого {raw_period}")
            else:
                inserted_id = insert_product(db, data_line)
                if inserted_id:
                    inserted_success.append((id, raw_code))
        row_count = len(raw_quotes)
        alog = f"Всего записей в raw таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_success)} расценок."
        ulog = f"Обновлено {len(updated_success)} расценок."
        none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)

    # удалить из Расценок записи период которых меньше чем максимальный период
    delete_last_period_product_row(db_filename, team=team, name=name)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    ic(db_name)

    transfer_raw_data_to_quotes(db_name)
