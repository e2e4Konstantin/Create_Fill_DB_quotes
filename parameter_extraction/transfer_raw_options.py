import sqlite3
from icecream import ic
from tools import read_csv_to_raw_table
from parameter_extraction.create_tables_options import create_tables_idx_options
from config import dbTolls
from sql_queries import sql_raw_data, sql_quotes_select
from sql_queries.OLD import sql_options_queries
from files_features import output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value, get_float_value


def _make_data_from_raw_option(db: dbTolls, raw_option: sqlite3.Row) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированным параметром.
        Выбирает нужные данные, находит в расценках запись владельца параметра.
        Возвращает кортеж с данными для вставки в таблицу Атрибутов tblOptions.
    """
    raw_quote_code = clear_code(raw_option["PRESSMARK"])
    raw_period = get_integer_value(raw_option["PERIOD"])
    raw_name = text_cleaning(raw_option['PARAMETER_TITLE']).capitalize()
    raw_left_border = get_float_value(raw_option['LEFT_BORDER'])
    raw_right_border = get_float_value(raw_option['RIGHT_BORDER'])
    raw_measurer = text_cleaning(raw_option['UNIT_OF_MEASURE'])
    raw_step = text_cleaning(raw_option['STEP'])
    raw_type = get_integer_value(raw_option['PARAMETER_TYPE'])

    # Найти расценку с шифром raw_cod в таблице Расценок
    quotes_cursor = db.go_execute(sql_quotes_select["select_quotes_row_code"], (raw_quote_code,))
    quote_row = quotes_cursor.fetchone() if quotes_cursor else None
    if quote_row:
        quote_period = quote_row['period']
        quote_id = quote_row['ID_tblQuote']
        if raw_period == quote_period:
            return quote_id, raw_name,  raw_left_border, raw_right_border, raw_measurer, raw_step, raw_type
        else:
            output_message_exit(
                f"Ошибка загрузки Параметра для расценки с шифром: {raw_quote_code!r}",
                f"период Атрибута {raw_period} не равен текущему периоду расценки {quote_period} ")
    else:
        output_message_exit(f"для Атрибута {tuple(raw_option)} не найдена Расценка",
                            f"шифр {raw_quote_code!r}")
    return tuple()


def _delete_option(db: dbTolls, id_option: int) -> int:
    """ Удаляет запись из таблицы Параметров по ID_Option."""
    del_cursor = db.go_execute(sql_options_queries["delete_option_id"], (id_option,))
    # mess = f"Из таблицы Параметров удалена запись с id = {id_option}"
    # ic(mess)
    return del_cursor.rowcount if del_cursor else 0


def _insert_option(db: dbTolls, option_data: tuple[int, str, float, float, str, str, int]) -> int:
    """ Вставляет Параметр в таблицу Параметров. """
    message = f"INSERT tblOptions id расценки: {option_data[0]} параметр: {option_data[1:]}"
    inserted_id = db.go_insert(sql_options_queries["insert_option"], option_data, message)
    if not inserted_id:
        output_message_exit(f"параметр {tuple(option_data)}", f"не добавлен в tblOptions")
    return inserted_id


def _get_option_id(db: dbTolls, option_data: tuple[int, str]) -> int:
    """ Ищет в таблице Параметров параметр по id Расценки и названию параметра."""
    options_cursor = db.go_execute(sql_options_queries["select_option_quote_id_name"], option_data)
    option_row = options_cursor.fetchone() if options_cursor else None
    if option_row:
        return option_row['ID_Option']
    return 0


def transfer_raw_table_to_options(db_filename: str):
    """ Записывает Параметры из сырой таблицы в рабочую таблицу tblOptions.
        Параметры которые надо добавить предварительно загружены в tblRawData.
        В таблице tblQuotes ищется расценка с шифром который указан для Параметра.
     """
    with dbTolls(db_filename) as db:
        raw_cursor = db.go_execute(sql_raw_data["select_rwd_all"])
        raw_rows = raw_cursor.fetchall() if raw_cursor else None
        if raw_rows:
            inserted_options = []
            deleted_options = []
            for row_count, row in enumerate(raw_rows):
                data = _make_data_from_raw_option(db, row)
                # ищем в таблице параметров совпадающий параметр
                same_id = _get_option_id(db, data[:2])
                if same_id > 0:
                    _delete_option(db, same_id)
                    deleted_options.append(data)
                _insert_option(db, data)
                inserted_options.append(data)
            row_count += 1
            alog = f"Всего raw записей в таблице параметров: {row_count}."
            ilog = f"Добавлено {len(inserted_options)} параметров в рабочую таблицу."
            dlog = f"Удалено {len(deleted_options)} совпадающих параметров."
            none_log = f"Непонятных записей: {row_count - len(inserted_options)}."
            ic(alog, ilog, dlog, none_log)
        else:
            output_message_exit(f"в RAW таблице для Параметров не найдено ни одной записи:", f"")


if __name__ == '__main__':
    import os

    data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    period = 68
    split_data = os.path.join(data_path, "Расценки_4_68_split_options.csv")

    ic(db_name)
    ic(split_data)

    # ! Удаляем таблицы для атрибутов и создаем новый
    create_tables_idx_options(db_name)

    # прочитать из csv файла данные для Параметров в таблицу tblRawData для периода period
    read_csv_to_raw_table(db_name, split_data, period)

    # заполнить Параметры данными из таблицы tblRawData
    transfer_raw_table_to_options(db_name)
