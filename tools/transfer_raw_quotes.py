import sqlite3
from icecream import ic

from config import dbTolls
from sql_queries import sql_quotes_insert_update, sql_raw_data, sql_catalog_select, sql_quotes_select
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value, get_float_value


def _get_catalog_id(period: int, code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице Каталога запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_select["select_catalog_period_code"], period, code)
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r} и период: {period}")
    return row_id

def _get_quote_id(period: int, code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице Расценок запись по шифру и периоду. """
    row_id = db.get_row_id(sql_quotes_select["select_quotes_period_code"], period, code)
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r} и период: {period}")
    return row_id


def _make_data_from_raw_quote(db: dbTolls, raw_quote: sqlite3.Row) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированными расценками.
        Выбирает нужные данные, проверяет их, находит в каталоге запись владельца расценки.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Расценок.
    """
    period = get_integer_value(raw_quote["PERIOD"])
    catalog_code = raw_quote['GROUP_WORK_PROCESS']
    if catalog_code is None:
        # указатель на корневую запись каталога
        holder_id = _get_catalog_id(period=0, code='0', db=db)
    else:
        catalog_code = clear_code(catalog_code)
        holder_id = _get_catalog_id(period=period, code=catalog_code, db=db)
        if not holder_id:
            output_message_exit(f"Ошибка поиска в каталоге, записи для расценки {raw_quote["PRESSMARK"]}",
                                f"В каталоге не найдена запись {catalog_code!r} период {period!r}")

    code = clear_code(raw_quote["PRESSMARK"])
    description = text_cleaning(raw_quote["TITLE"]).capitalize()
    measurer = text_cleaning(raw_quote["UNIT_OF_MEASURE"])
    salary = get_float_value(raw_quote["SALARY"])
    operation_of_machines = get_float_value(raw_quote["OPERATION_OF_MACHINES"])
    cost_of_material = get_float_value(raw_quote["COST_OF_MATERIAL"])
    data = (holder_id, period, code, description, measurer, salary, operation_of_machines, cost_of_material)
    return data



def _update_quote(db: dbTolls, quote_id: int, quote: sqlite3.Row) -> int | None:
    """ Получает строку из Сырой таблице с расценками. Вставляет в рабочую таблицу. """
    data = _make_data_from_raw_quote(db, quote) + (quote_id, )
    db.go_execute(sql_quotes_insert_update["update_quote"], data)
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
    """ Записывает расценки из сырой базы в рабочую """
    with dbTolls(db_filename) as db:
        result = db.go_execute(sql_raw_data["select_rwd_all"])
        if result:
            inserted_success = []
            updated_success = []
            for row in result.fetchall():
                code = row["PRESSMARK"]
                period = row["PERIOD"]
                row_id = db.get_row_id(sql_quotes_select["select_quotes_period_code"], (period, code))
                if row_id:
                    id = _update_quote(db, row_id, row)
                    if id:
                        updated_success.append((id, code))
                else:
                    id = _insert_raw_quote(db, row)
                    if id:
                        inserted_success.append((id, code, period))

            ilog = f"Добавлено {len(inserted_success)} расценок."
            ulog = f"Обновлено {len(updated_success)} расценок."
            ic(ilog, ulog)
        else:
            output_message(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                           f"")


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")
    ic(db_name)
    transfer_raw_table_data_to_quotes(db_name)
