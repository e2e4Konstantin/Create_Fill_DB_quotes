import sqlite3
import pandas as pd
from icecream import ic
from config import dbTolls, items_catalog, DirectoryItem
from sql_queries import (
    sql_items_queries, sql_raw_queries, sql_catalog_queries
)
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, title_catalog_extraction, get_integer_value
from tools.shared_features import (
    get_sorted_directory_items, get_catalog_id_by_code, delete_catalog_rows_with_old_period
)


def _get_directory_id(item_name: str, db: dbTolls) -> int | None:
    """ Ищет в справочнике clip запись с именем item_name, возвращает id """
    directory_name = "clip"
    id_catalog_items = db.get_row_id(
        sql_items_queries["select_items_team_code"], (directory_name, item_name,)
    )
    if id_catalog_items is None:
        output_message(f"В {directory_name} справочнике не найден:", f"название: {item_name!r}")
    return id_catalog_items


def _make_data_from_raw_catalog(db: dbTolls, raw_catalog_row: sqlite3.Row, item: DirectoryItem) -> tuple | None:
    """ Из строки raw_catalog_row таблицы tblRawData с данными для Каталога.
        Выбирает данные, проверяет их, находит в Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # ic(tuple(row))
    # в Каталоге ищем родителя
    raw_parent_code = raw_catalog_row["PARENT_PRESSMARK"]
    if raw_parent_code is None:
        parent_id = get_catalog_id_by_code(db=db, code='0000')
    else:
        parent_code = clear_code(raw_parent_code)
        parent_id = get_catalog_id_by_code(db=db, code=parent_code)
    if parent_id and str(item.id):
        period = get_integer_value(raw_catalog_row["PERIOD"])
        code = clear_code(raw_catalog_row["PRESSMARK"])
        description = title_catalog_extraction(raw_catalog_row["TITLE"], item.item_name)
        # ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
        data = (parent_id, period, code, description, item.id)
        # ic(data)
        return data
    else:
        output_message_exit(f"В Каталоге не найдена родительская запись с шифром: {raw_parent_code!r}",
                            f"для {tuple(raw_catalog_row)} ")
    return None


def _update_catalog(db: dbTolls, catalog_id: int, raw_table_row: sqlite3.Row, item: DirectoryItem) -> int | None:
    """ Формирует строку из Сырой таблицы. Изменяет catalog_id запись в таблице Каталога. """
    data = _make_data_from_raw_catalog(db, raw_table_row, item)
    # ID_parent, period, code, description, FK_tblCatalogs_tblItems, ID_tblCatalog, period
    data = (data + (catalog_id, data[1]))

    db.go_execute(sql_catalog_queries["update_catalog_id_period"], data)
    count = db.go_execute(sql_catalog_queries["select_changes"])
    return count.fetchone()['changes'] if count else None


def _insert_raw_catalog(db: dbTolls, raw_table_row: sqlite3.Row, item: DirectoryItem) -> int | None:
    """ Формирует строку из Сырой таблицы. Вставляет новую запись в таблицу Каталога. """
    data = _make_data_from_raw_catalog(db, raw_table_row, item)
    message = f"INSERT tblCatalog {item.item_name!r} шифр {data[2]!r} период: {data[1]!r}"
    inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
    if not inserted_id:
        output_message(f"запись {tuple(raw_table_row)}", f"НЕ добавлена в Каталог.")
        return None
    return inserted_id


def _get_raw_data_items(db: dbTolls, item: DirectoryItem) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из сырой таблицы у которых шифр соответствует паттерну для item типа записей. """
    raw_cursor = db.go_execute(sql_raw_queries["select_rwd_code_regexp"], (item.re_pattern,))
    results = raw_cursor.fetchall() if raw_cursor else None
    if not results:
        output_message_exit(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                            f"{item.item_name!r}, {item.re_pattern}")
        return None
    return results


def _transfer_raw_item_to_catalog(item: DirectoryItem, db_filename: str):
    """ Записывает все значения типа item_name в каталог из таблицы с исходными данными
        в таблицу каталога и создает ссылки на родителей.
        Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
        Период записываем только если он больше либо равен предыдущему.
    """
    with (dbTolls(db_filename) as db):
        raw_data = _get_raw_data_items(db, item)
        if not raw_data:
            return None
        inserted_success, updated_success = [], []
        for row_count, row in enumerate(raw_data):
            raw_code = clear_code(row["PRESSMARK"])
            raw_period = get_integer_value(row["PERIOD"])
            catalog_cursor = db.go_execute(sql_catalog_queries["select_catalog_id_code"], (raw_code,))
            catalog_row = catalog_cursor.fetchone() if catalog_cursor else None
            if catalog_row:
                row_period = catalog_row['period']
                row_id = catalog_row['ID_tblCatalog']
                if raw_period >= row_period:
                    changed_count = _update_catalog(db, row_id, row, item)
                    if changed_count:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка загрузки данных в Каталог, записи с шифром: {raw_code!r}",
                        f"текущий период каталога {row_period} больше загружаемого {raw_period}")
            else:
                work_id = _insert_raw_catalog(db, row, item)
                if work_id:
                    inserted_success.append((id, raw_code))
        alog = f"Для {item.item_name!r}:Всего пройдено записей в raw таблице: {row_count + 1}."
        ilog = f"Добавлено {len(inserted_success)}."
        ulog = f"Обновлено {len(updated_success)}."
        none_log = f"Непонятных записей: {row_count + 1 - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)


def transfer_raw_table_data_to_catalog(operating_db: str):
    """ Заполняет таблицу Каталога данными из RAW таблицы. Каталог заполняется последовательно,
        с самого старшего элемента. В соответствии с иерархией Справочника 'quotes'.
        Иерархия задается родителями в классе ItemCatalogDirectory.
    """
    ic()
    # получить отсортированные по иерархии Справочник 'quotes'
    dir_catalog = get_sorted_directory_items(operating_db, directory_name='quotes')
    ic(dir_catalog)
    for item in dir_catalog[1:]:
        _transfer_raw_item_to_catalog(item, operating_db)
    # удалить из Каталога записи период которых меньше чем текущий период
    delete_catalog_rows_with_old_period(operating_db)


if __name__ == '__main__':
    import os

    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    transfer_raw_table_data_to_catalog(db_name)
