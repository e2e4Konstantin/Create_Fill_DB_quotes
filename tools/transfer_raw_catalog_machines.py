import sqlite3

from icecream import ic
from config import dbTolls, items_catalog
from sql_queries import (
    sql_items_queries, sql_raw_queries, sql_catalog_queries
)
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, title_catalog_extraction, get_integer_value
from tools.get_directory_items import get_sorted_directory_items


def _get_catalog_id(code: str, db: dbTolls) -> int | None:
    """ Ищет в Каталоге запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_code"], (code,))
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return row_id


def _get_directory_id(item_name: str, db: dbTolls) -> int | None:
    """ Ищет в справочнике clip запись с именем item_name, возвращает id """
    directory_name = "clip"
    id_catalog_items = db.get_row_id(
        sql_items_queries["select_items_team_code"], (directory_name, item_name,)
    )
    if id_catalog_items is None:
        output_message(f"В {directory_name} справочнике не найден:", f"название: {item_name!r}")
    return id_catalog_items


def _make_data_from_raw_catalog(db: dbTolls, raw_catalog_row: sqlite3.Row, item: tuple[int, str, str]) -> tuple:
    """ Из строки raw_catalog_row таблицы tblRawData с данными для Каталога.
        Выбирает данные, проверяет их, находит в Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # ic(tuple(row))
    # в Каталоге ищем родителя
    raw_parent_code = raw_catalog_row["PARENT_PRESSMARK"]
    if raw_parent_code is None:
        parent_id = _get_catalog_id(code='0000', db=db)
    else:
        parent_code = clear_code(raw_parent_code)
        parent_id = _get_catalog_id(code=parent_code, db=db)
    # id записи элемента каталога
    id_items = str(item[0])
    if parent_id and id_items:
        period = get_integer_value(raw_catalog_row["PERIOD"])
        code = clear_code(raw_catalog_row["PRESSMARK"])
        description = title_catalog_extraction(raw_catalog_row["TITLE"], item[1])
        # ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
        data = (parent_id, period, code, description, int(item[0]))
        # ic(data)
        return data
    else:
        output_message_exit(f"В Каталоге не найдена родительская запись с шифром: {raw_parent_code!r}",
                            f"для {tuple(raw_catalog_row)} ")
    return tuple()


def _update_catalog(db: dbTolls, catalog_id: int, raw_table_row: sqlite3.Row, item: tuple[int, str, str]) -> int | None:
    """ Формирует строку из Сырой таблицы. Изменяет catalog_id запись в таблице Каталога. """
    data = _make_data_from_raw_catalog(db, raw_table_row, item)
    # ID_parent, period, code, description, FK_tblCatalogs_tblItems, ID_tblCatalog, period
    data = (data + (catalog_id, data[1]))

    db.go_execute(sql_catalog_queries["update_catalog_id_period"], data)
    count = db.go_execute(sql_catalog_queries["select_changes"])
    return count.fetchone()['changes'] if count else None


def _insert_raw_catalog(db: dbTolls, raw_table_row: sqlite3.Row, item: tuple[int, str, str]) -> int | None:
    """ Формирует строку из Сырой таблицы. Вставляет новую запись в таблицу Каталога. """
    data = _make_data_from_raw_catalog(db, raw_table_row, item)
    message = f"INSERT tblCatalog {item[1]!r} шифр {data[2]!r} период: {data[1]!r}"
    inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
    if not inserted_id:
        output_message(f"запись {tuple(raw_table_row)}", f"НЕ добавлена в Каталог.")
        return None
    return inserted_id


def _get_item_pattern(db: dbTolls, directory_name: str, item_name: str) -> str | None:
    items_cursor = db.go_execute(sql_items_queries["select_items_all_team_name"], (directory_name, item_name))
    item = items_cursor.fetchone() if items_cursor else None
    return item['re_pattern'] if item else None


def _get_raw_data_items(db: dbTolls, directory_name: str, item_name: str) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из сырой таблицы у которых шифр соответствует паттерну для этого типа записей. """

    # получить паттерн из справочника
    item_pattern = _get_item_pattern(db, directory_name, item_name)

    if item_pattern is None:
        return None
    raw_cursor = db.go_execute(sql_raw_queries["select_rwd_code_regexp"], (item_pattern,))
    results = raw_cursor.fetchall() if raw_cursor else None
    if not results:
        output_message_exit(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                       f"{items_catalog[item_name].name!r}, {item_pattern}")
        return None
    return results


def _transfer_raw_item_to_catalog(item: tuple[int, str, str], db_filename: str):
    """ Записывает все значения типа item_name в каталог из таблицы с исходными данными
        в таблицу каталога и создает ссылки на родителей.
        Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
        Период записываем только если он больше либо равен предыдущему.
    """
    with (dbTolls(db_filename) as db):
        raw_data = _get_raw_data_items(db, item[2], item[1])
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
        alog = f"Для {item[1]!r}:Всего пройдено записей в raw таблице: {row_count + 1}."
        ilog = f"Добавлено {len(inserted_success)}."
        ulog = f"Обновлено {len(updated_success)}."
        none_log = f"Непонятных записей: {row_count + 1 - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)


def _delete_last_period_catalog_row(db_filename: str):
    """ Удалить все записи у которых период < максимального. """
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_catalog_queries["select_catalog_max_period"])
        max_period = work_cursor.fetchone()['max_period'] if work_cursor else None
        if max_period is None:
            output_message_exit(f"при получении максимального периода Каталога", f"{max_period=}")
            return
        ic(max_period)

        db.go_execute(sql_catalog_queries["update_catalog_period_main_row"], (max_period,))
        changes = db.go_execute(sql_catalog_queries["select_changes"]).fetchone()['changes']
        message = f"обновлен период {max_period} головной записи: {changes}"
        ic(message)

        deleted_cursor = db.go_execute(sql_catalog_queries["select_count_last_period"], (max_period,))
        number_past = deleted_cursor.fetchone()[0]
        mess = f"Из Каталога будут удалены {number_past} записей у которых период меньше текущего: {max_period}"
        ic(mess)
        #
        deleted_cursor = db.go_execute(sql_catalog_queries["delete_catalog_last_periods"], (max_period,))
        mess = f"Из Каталога удалено {deleted_cursor.rowcount} записей с period < {max_period}"
        ic(mess)




def transfer_raw_data_to_catalog_machines(operating_db: str):
    """ Заполняет каталог данными из таблицы с RAW данными для МАШИН Глава 1 в таблицу Каталога.
        Каталог заполняется в соответствии с иерархией элементов каталога.
        Иерархия задается родителями в классе ItemCatalogDirectory.
    """
    # получить отсортированный справочник 'machines'
    dir_catalog = get_sorted_directory_items(operating_db, directory_name='machines')
    ic(dir_catalog)

    for item in dir_catalog[1:]:
        _transfer_raw_item_machines_to_catalog(item, operating_db)
    #
    # # удалить из Каталога записи период которых меньше чем текущий период
    # _delete_last_period_catalog_row(operating_db)


if __name__ == '__main__':
    import os

    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    transfer_raw_data_to_catalog_machines(db_name)
