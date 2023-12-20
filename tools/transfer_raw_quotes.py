import sqlite3
from icecream import ic

from config import dbTolls, teams
from sql_queries import sql_raw_queries, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value, get_float_value


def _get_catalog_id(db: dbTolls, period: int, code: str) -> int | None:
    """ Ищем в таблице Каталога запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_period_code"], (period, code))
    if row_id is None:
        output_message(f"В Каталоге не найдена запись", f"шифр {code!r}, период: {period}")
    return row_id


def _get_quote_id(db: dbTolls, period: int, code: str) -> int | None:
    """ Ищем в таблице Расценок запись по шифру и периоду. """
    row_id = db.get_row_id(sql_products_queries["select_product_id_period_code"], (period, code))
    if row_id is None:
        output_message(f"В Расценках не найдена запись", f"шифр {code!r} период: {period}")
    return row_id


def _make_data_from_raw_quote(db: dbTolls, raw_quote: sqlite3.Row, item_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированной расценкой и id типа записи.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    raw_period = get_integer_value(raw_quote["PERIOD"])
    raw_code = raw_quote['PRESSMARK']
    holder_code = raw_quote['GROUP_WORK_PROCESS']
    if holder_code is None:
        # указатель на корневую запись каталога
        holder_id = _get_catalog_id(db=db, period=0, code='0000')
    else:
        holder_code = clear_code(holder_code)
        # в Каталоге!!! ищем родительскую запись с шифром holder_cod
        holder_id = _get_catalog_id(db=db, period=raw_period, code=holder_code)
        if holder_id:
            code = clear_code(raw_quote["PRESSMARK"])
            description = text_cleaning(raw_quote["TITLE"]).capitalize()
            measurer = text_cleaning(raw_quote["UNIT_OF_MEASURE"])
            # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, period, code, description, measurer, full_code
            data = (holder_id, item_id, raw_period, code, description, measurer, "")
            return data
        else:
            output_message_exit(f"Для расценки {raw_code!r} в Каталоге не найдена родительская запись",
                                f"шифр {holder_code!r}")
    return None


def _update_quote(db: dbTolls, type_id: int, quote_id: int, raw_quote: sqlite3.Row) -> int | None:
    """ Получает строку из Сырой таблицы с расценками. Обновляет расценку с quote_id. """
    data = _make_data_from_raw_quote(db, raw_quote, type_id) + (quote_id,)
    db.go_execute(sql_products_queries["update_product_id"], data)
    count = db.go_execute(sql_products_queries["select_changes"])
    return count.fetchone()['changes'] if count else None


def _insert_raw_quote(db: dbTolls, type_id: int, raw_quote: sqlite3.Row) -> int | None:
    """ Получает строку из Сырой таблицы с расценками. Вставляет расценку в таблицу tblProducts. """
    data = _make_data_from_raw_quote(db, raw_quote, type_id)
    message = f"INSERT tblQuotes шифр {data[2]!r} период: {data[1]}"
    inserted_id = db.go_insert(sql_products_queries["insert_product"], data, message)
    if not inserted_id:
        output_message(f"расценка {tuple(raw_quote)}", f"не добавлена в tblQuotes")
        return None
    return inserted_id


def _delete_last_period_quotes_row(db_filename: str):
    """ Удалить все записи у которых период < максимального.  """
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_products_queries["select_products_max_period"])
        max_period = work_cursor.fetchone() if work_cursor else None
        if max_period is None:
            output_message_exit(f"Что то пошло не так при получении максимального периода Расценок",
                                f"{sql_products_queries['select_products_max_period']!r}")
            return
        current_period = max_period['max_period']
        ic(current_period)
        deleted_cursor = db.go_execute(sql_products_queries["select_products_count_period_less"], (current_period,))
        message = f"Будут удалены {deleted_cursor.fetchone()[0]} расценок с периодом меньше текущего: {current_period}"
        ic(message)
        deleted_cursor = db.go_execute(sql_products_queries["delete_products_last_periods"], (current_period,))
        mess = f"Из Расценок удалено {deleted_cursor.rowcount} записей с period < {current_period}"
        ic(mess)


def transfer_raw_data_to_quotes(db_filename: str):
    """ Записывает расценки из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется расценка с таким же шифром, если такая есть то она обновляется,
        если не найдена то вставляется новая.
     """
    with dbTolls(db_filename) as db:
        # получить все строки raw таблицы
        raw_data = db.go_select(sql_raw_queries["select_rwd_all"])
        if not raw_data:
            output_message_exit(f"в RAW таблице с Расценками нет записей:",f"tblRawData пустая")
            return None
        # получить id типа для расценки
        target_type_id = db.get_row_id(sql_items_queries["select_item_id_team_name"], ("units", "quote"))
        inserted_success, updated_success = [], []
        for row_count, row in enumerate(raw_data):
            raw_code = clear_code(row["PRESSMARK"])
            raw_period = get_integer_value(row["PERIOD"])
            # Найти запись с шифром raw_cod в таблице расценок tblProducts
            result = db.go_select(sql_products_queries["select_products_item_code"], (raw_code,))
            if result:
                quote = result[0]
                if raw_period >= quote['period'] and target_type_id == quote['FK_tblProducts_tblItems']:
                    count_updated = _update_quote(db, target_type_id, quote['ID_tblProduct'], row)
                    if count_updated:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка Обновления Расценки: {raw_code!r} или item_type не совпадает {target_type_id}",
                        f"период Расценки {quote['period']} старше загружаемого {raw_period}")
            else:
                inserted_id = _insert_raw_quote(db, target_type_id, row)
                if inserted_id:
                    inserted_success.append((id, raw_code))
        row_count += 1
        alog = f"Всего записей в raw таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_success)} расценок."
        ulog = f"Обновлено {len(updated_success)} расценок."
        none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)

    # удалить из Расценок записи период которых меньше чем максимальный период
    _delete_last_period_quotes_row(db_filename)


if __name__ == '__main__':
    import os

    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    ic(db_name)

    transfer_raw_data_to_quotes(db_name)
