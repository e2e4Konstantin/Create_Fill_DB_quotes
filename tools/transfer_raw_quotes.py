import sqlite3
from icecream import ic

from config import dbTolls
from sql_queries import sql_quotes_insert_update, sql_raw_data, sql_catalog_select, sql_quotes_select, sql_quotes_delete
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value, get_float_value


def _get_catalog_id(period: int, code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице Каталога запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_select["select_catalog_period_code"], period, code)
    if row_id is None:
        output_message(f"В Каталоге не найдена запись", f"шифр {code!r}, период: {period}")
    return row_id


def _get_quote_id(period: int, code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице Расценок запись по шифру и периоду. """
    row_id = db.get_row_id(sql_quotes_select["select_quotes_period_code"], period, code)
    if row_id is None:
        output_message(f"В Каталоге не найдена запись", f"шифр {code!r} период: {period}")
    return row_id


def _make_data_from_raw_quote(db: dbTolls, raw_quote: sqlite3.Row) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированными расценками.
        Выбирает нужные данные, проверяет их, находит в каталоге запись владельца расценки.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Расценок.
    """
    raw_period = get_integer_value(raw_quote["PERIOD"])
    raw_code = raw_quote['PRESSMARK']
    holder_code = raw_quote['GROUP_WORK_PROCESS']
    if holder_code is None:
        # указатель на корневую запись каталога
        holder_id = _get_catalog_id(period=0, code='0', db=db)
    else:
        holder_code = clear_code(holder_code)
        # в Каталоге ищем Родительскую запись с шифром holder_cod
        work_cursor = db.go_execute(sql_catalog_select["select_catalog_row_code"], (holder_code,))
        work_row = work_cursor.fetchone() if work_cursor else None
        if work_row:
            # holder_period = work_row['period']
            holder_id = work_row['ID_tblCatalog']
            code = clear_code(raw_quote["PRESSMARK"])
            description = text_cleaning(raw_quote["TITLE"]).capitalize()
            measurer = text_cleaning(raw_quote["UNIT_OF_MEASURE"])
            salary = get_float_value(raw_quote["SALARY"])
            operation_of_machines = get_float_value(raw_quote["OPERATION_OF_MACHINES"])
            cost_of_material = get_float_value(raw_quote["COST_OF_MATERIAL"])
            data = (holder_id, raw_period, code, description, measurer, salary, operation_of_machines, cost_of_material)
            return data
        else:
            output_message_exit(f"Для расценки {raw_code!r} в Каталоге не найдена родительская запись",
                                f"шифр {holder_code!r}")
    return tuple()


def _update_quote(db: dbTolls, quote_id: int, quote: sqlite3.Row) -> int | None:
    """ Получает строку из Сырой таблице с расценками. Вставляет в рабочую таблицу. """
    data = _make_data_from_raw_quote(db, quote) + (quote_id,)
    db.go_execute(sql_quotes_insert_update["update_quote_id"], data)
    return db.go_execute("""SELECT CHANGES();""")


def _insert_raw_quote(db: dbTolls, raw_quote: sqlite3.Row) -> int | None:
    data = _make_data_from_raw_quote(db, raw_quote)
    # ic(data)
    message = f"INSERT tblQuotes шифр {data[2]!r} период: {data[1]}"
    inserted_id = db.go_insert(sql_quotes_insert_update["insert_quote"], data, message)
    if not inserted_id:
        output_message(f"расценка {tuple(raw_quote)}", f"не добавлена в tblQuotes")
        return None
    return inserted_id


def transfer_raw_table_data_to_quotes(db_filename: str):
    """ Записывает расценки из сырой базы в рабочую.
        Расценки которые надо добавить предварительно загружены в tblRawData.
        В рабочей таблице tblQuotes ищется расценка с таким же шифром, если такая есть то она обновляется,
        нет добавляется.
     """
    with dbTolls(db_filename) as db:
        raw_cursor = db.go_execute(sql_raw_data["select_rwd_all"])
        if raw_cursor:
            inserted_success = []
            updated_success = []
            for raw_count, row in enumerate(raw_cursor.fetchall()):
                raw_code = clear_code(row["PRESSMARK"])
                raw_period = get_integer_value(row["PERIOD"])
                # Найти запись с шифром raw_cod в рабочей таблице расценок
                work_cursor = db.go_execute(sql_quotes_select["select_quotes_row_code"], (raw_code,))
                work_row = work_cursor.fetchone() if work_cursor else None
                if work_row:
                    work_period = work_row['period']
                    work_id = work_row['ID_tblQuote']
                    if raw_period >= work_period:
                        work_id = _update_quote(db, work_id, row)
                        if work_id:
                            updated_success.append((id, raw_code))
                    else:
                        output_message_exit(
                            f"Ошибка загрузки Расценки с шифром: {raw_code!r}",
                            f"текущий период Расценки {work_period} старше загружаемого {raw_period}")
                else:
                    work_id = _insert_raw_quote(db, row)
                    if work_id:
                        inserted_success.append((id, raw_code))
            raw_count += 1
            alog = f"Всего записей в raw таблице: {raw_count}."
            ilog = f"Добавлено {len(inserted_success)} расценок."
            ulog = f"Обновлено {len(updated_success)} расценок."
            none_log = f"Непонятных записей: {raw_count - (len(updated_success) + len(inserted_success))}."
            ic(alog, ilog, ulog, none_log)
        else:
            output_message(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                           f"")

    # удалить из Расценок записи период которых меньше чем максимальный период
    _delete_last_period_quotes_row(db_filename)


def _delete_last_period_quotes_row(db_filename: str):
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_quotes_select["select_quotes_max_period"])
        max_period = work_cursor.fetchone() if work_cursor else None
        if max_period:
            current_period = max_period['max_period']
            ic(current_period)
            deleted_cursor = db.go_execute(sql_quotes_select["select_quotes_count_period_less"], (current_period,))
            mess = f"Из Расценок будут удалены {deleted_cursor.fetchone()[0]} записей у которых период меньше текущего: {current_period}"
            ic(mess)
            deleted_cursor = db.go_execute(sql_quotes_delete["delete_quotes_last_periods"], (current_period, ))
            mess = f"Из Расценок удалено {deleted_cursor.rowcount} записей с period < {current_period}"
            ic(mess)
        else:
            output_message_exit(f"Что то пошло не так при получении максимального периода Расценок",
                                f"{sql_catalog_select['select_max_period']!r}")


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")
    ic(db_name)
    # transfer_raw_table_data_to_quotes(db_name)
    _delete_last_period_quotes_row(db_name)