import sqlite3
from icecream import ic
from tools import read_csv_to_raw_table
from parameter_extraction.create_tables_attributes import create_tables_idx_attributes
from config import dbTolls
from sql_queries import sql_raw_data, sql_quotes_select, sql_attributes_delete, sql_attributes_insert_update, \
    sql_attributes_select
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value, get_float_value


def _make_data_from_raw_attribute(db: dbTolls, raw_attribute: sqlite3.Row) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированным атрибутом.
        Выбирает нужные данные, находит в расценках запись владельца атрибута.
        Возвращает кортеж с данными для вставки в таблицу Атрибутов tblAttributes.
    """
    raw_code = clear_code(raw_attribute["PRESSMARK"])
    raw_period = get_integer_value(raw_attribute["PERIOD"])
    raw_name = text_cleaning(raw_attribute['ATTRIBUTE_TITLE']).capitalize()
    raw_value = text_cleaning(raw_attribute['VALUE'])

    # Найти расценку с шифром raw_cod в таблице Расценок
    quotes_cursor = db.go_execute(sql_quotes_select["select_quotes_row_code"], (raw_code,))
    quote_row = quotes_cursor.fetchone() if quotes_cursor else None
    if quote_row:
        quote_period = quote_row['period']
        quote_id = quote_row['ID_tblQuote']
        if raw_period == quote_period:
            return quote_id, raw_name, raw_value
        else:
            output_message_exit(
                f"Ошибка загрузки Атрибута для расценки с шифром: {raw_code!r}",
                f"период Атрибута {raw_period} не равен текущему периоду расценки {quote_period} ")
    else:
        output_message_exit(f"для Атрибута {tuple(raw_attribute)} не найдена Расценка",
                            f"шифр {raw_code!r}")
    return tuple()


def _delete_attribute(db: dbTolls, id_attribute: int) -> int:
    """ Удаляет запись из таблицы атрибутов с ID_Attribute = id_attribute."""
    del_cursor = db.go_execute(sql_attributes_delete["delete_attributes_id"], (id_attribute,))
    # mess = f"Из таблицы Атрибутов удалена запись с id = {id_attribute}"
    # ic(mess)
    return del_cursor.rowcount if del_cursor else 0


def _insert_attribute(db: dbTolls, data: tuple[int, str, str]) -> int:
    """ Вставляет атрибут в таблицу Атрибутов """
    message = f"INSERT tblAttributes id расценки: {data[0]} атрибут: {data[1]!r} {data[2]!r}"
    inserted_id = db.go_insert(sql_attributes_insert_update["insert_attribute"], data, message)
    if not inserted_id:
        output_message_exit(f"атрибут {tuple(data)}", f"не добавлен в tblAttributes")
    return inserted_id


def _get_attribute_id(db: dbTolls, attribute_data: tuple[int, str]) -> int:
    """ Ищет в таблице Атрибутов атрибут по id Расценки и названию атрибута."""
    attributes_cursor = db.go_execute(sql_attributes_select["select_attributes_quote_id_name"], attribute_data)
    attribute_row = attributes_cursor.fetchone() if attributes_cursor else None
    if attribute_row:
        return attribute_row['ID_Attribute']
    return 0


def transfer_raw_table_to_attributes(db_filename: str):
    """ Записывает атрибуты из сырой таблицы в рабочую tblAttributes.
        Атрибуты которые надо добавить предварительно загружены в tblRawData.
        В таблице tblQuotes ищется расценка с шифром который указан для Атрибута.
     """
    with dbTolls(db_filename) as db:
        raw_cursor = db.go_execute(sql_raw_data["select_rwd_all"])
        raw_rows = raw_cursor.fetchall() if raw_cursor else None
        if raw_rows:
            inserted_attributes = []
            deleted_attributes = []
            for row_count, row in enumerate(raw_rows):
                data = _make_data_from_raw_attribute(db, row)
                # ищем в таблице атрибутов совпадающий атрибут
                same_id = _get_attribute_id(db, data[:2])
                if same_id > 0:
                    _delete_attribute(db, same_id)
                    deleted_attributes.append(data)
                _insert_attribute(db, data)
                inserted_attributes.append(data)
            row_count += 1
            alog = f"Всего raw записей в таблице: {row_count}."
            ilog = f"Добавлено {len(inserted_attributes)} атрибутов."
            dlog = f"Удалено {len(deleted_attributes)} совпадающих атрибутов."
            none_log = f"Непонятных записей: {row_count - len(inserted_attributes)}."
            ic(alog, ilog, dlog, none_log)
        else:
            output_message_exit(f"в RAW таблице для Атрибутов не найдено ни одной записи:", f"")


if __name__ == '__main__':
    import os

    data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    period = 68
    split_data = os.path.join(data_path, "Расценки_4_68_split_attributes.csv")

    ic(db_name)
    ic(split_data)

    # ! Удаляем таблицы для атрибутов и создаем новый
    # create_tables_idx_attributes(db_name)

    # прочитать из csv файла данные для Атрибутов в таблицу tblRawData для периода period
    # read_csv_to_raw_table(db_name, split_data, period)

    # заполнить Атрибуты данными из таблицы tblRawData
    transfer_raw_table_to_attributes(db_name)
