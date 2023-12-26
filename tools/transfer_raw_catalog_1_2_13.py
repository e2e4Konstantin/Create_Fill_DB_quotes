import sqlite3

from icecream import ic
from config import dbTolls, items_catalog, DirectoryItem
from sql_queries import (
    sql_items_queries, sql_raw_queries, sql_catalog_queries
)
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, title_catalog_extraction, get_integer_value
from tools.shared_features import (
    get_sorted_directory_items, get_catalog_id_by_code, delete_catalog_old_period_for_parent_code
)


def _make_data_from_raw_catalog_material(db: dbTolls, raw_catalog_row: sqlite3.Row,
                                         item: DirectoryItem) -> tuple | None:
    """ Из строки raw_catalog_row таблицы tblRawData с raw данными для Каталога.
        Выбирает данные, проверяет их, находит в Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # в Каталоге ищем родителя
    raw_parent_id = raw_catalog_row["PARENT"]
    if raw_parent_id is None:
        parent_id = get_catalog_id_by_code(db=db, code='0000')
    else:
        select = db.go_select(sql_raw_queries["select_rwd_material_cmt_id"], (raw_parent_id,))
        parent_code = clear_code(select[0]['CMT']) if select else None
        parent_id = get_catalog_id_by_code(db=db, code=parent_code) if parent_code else None
    if parent_id and str(item.id):
        period = get_integer_value(raw_catalog_row["PERIOD"])
        code = clear_code(raw_catalog_row["CMT"])
        description = title_catalog_extraction(raw_catalog_row["TITLE"], item.re_prefix)
        # ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
        data = (parent_id, period, code, description, item.id)
        return data
    else:
        output_message_exit(f"В Каталоге не найдена родительская запись с шифром: {raw_parent_code!r}",
                            f"для {tuple(raw_catalog_row)} ")
    return None


def _update_material_catalog(db: dbTolls, catalog_id: int, raw_table_row: sqlite3.Row,
                             item: DirectoryItem) -> int | None:
    """ Формирует строку из Сырой таблицы. Изменяет catalog_id запись в таблице Каталога. """
    data = _make_data_from_raw_catalog_material(db, raw_table_row, item)
    # ID_parent, period, code, description, FK_tblCatalogs_tblItems, ID_tblCatalog, period
    data = (data + (catalog_id, data[1]))

    db.go_execute(sql_catalog_queries["update_catalog_id_period"], data)
    count = db.go_execute(sql_catalog_queries["select_changes"])
    return count.fetchone()['changes'] if count else None


def _insert_material_raw_catalog(db: dbTolls, raw_table_row: sqlite3.Row, item: DirectoryItem) -> int | None:
    """ Формирует строку из Сырой таблицы. Вставляет новую запись в таблицу Каталога. """
    data = _make_data_from_raw_catalog_material(db, raw_table_row, item)
    message = f"INSERT tblCatalog {item.item_name!r} шифр {data[2]!r} период: {data[1]!r}"
    inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
    if not inserted_id:
        output_message(f"запись {tuple(raw_table_row)}", f"НЕ добавлена в Каталог.")
        return None
    return inserted_id


def _get_raw_data_items(db: dbTolls, item: DirectoryItem) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из сырой таблицы у которых шифр соответствует паттерну для item типа записей. """
    raw_cursor = db.go_execute(
        sql_raw_queries["select_rwd_materials_items_re_catalog"], (item.re_pattern,)
    )
    results = raw_cursor.fetchall() if raw_cursor else None
    if not results:
        output_message_exit(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                            f"{item.item_name!r}, {item.re_pattern}")
        return None
    return results


def _transfer_raw_item_to_catalog(item: DirectoryItem, db_filename: str):
    """ Записывает все значения item в каталог из таблицы с исходными данными tblRawData.
        Создает ссылки на родителей.
        Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
        Период записываем только если он больше либо равен предыдущему.
    """
    with (dbTolls(db_filename) as db):
        raw_data = _get_raw_data_items(db, item)
        if not raw_data:
            return None
        inserted_success, updated_success = [], []
        for row_count, row in enumerate(raw_data):
            raw_code = clear_code(row["CMT"])
            raw_period = get_integer_value(row["PERIOD"])
            catalog_cursor = db.go_execute(sql_catalog_queries["select_catalog_id_code"], (raw_code,))
            catalog_row = catalog_cursor.fetchone() if catalog_cursor else None
            if catalog_row:
                row_period = catalog_row['period']
                row_id = catalog_row['ID_tblCatalog']
                if raw_period >= row_period:
                    changed_count = _update_material_catalog(db, row_id, row, item)
                    if changed_count:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка загрузки данных в Каталог, записи с шифром: {raw_code!r}",
                        f"текущий период каталога {row_period} больше загружаемого {raw_period}")
            else:
                work_id = _insert_material_raw_catalog(db, row, item)
                if work_id:
                    inserted_success.append((id, raw_code))
        alog = f"Для {item[1]!r}:Всего пройдено записей в raw таблице: {row_count + 1}."
        ilog = f"Добавлено {len(inserted_success)}."
        ulog = f"Обновлено {len(updated_success)}."
        none_log = f"Непонятных записей: {row_count + 1 - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)


def transfer_raw_data_to_catalog(db_name: str, directory: str, main_code: str):
    """ Заполняет каталог данными из таблицы с RAW данными для справочника 'directory' в таблицу Каталога.
        Каталог заполняется в соответствии с иерархией элементов в справочнике 'directory'.
        Сортирует справочник в соответствии с иерархией. Иерархия задается родителями в классе ItemCatalogDirectory.
        Удаляет младшие периоды для каталога из элементов справочника 'directory'.
        Удалить из Каталога записи главы 'main_code' период которых меньше чем текущий период.
         (максимальный на данный в каталоге записей начиная с 'main_code')
    """
    dir_catalog = get_sorted_directory_items(db_name, directory_name=directory)
    ic(dir_catalog)
    for item in dir_catalog[1:]:
        _transfer_raw_item_to_catalog(item, db_name)
    delete_catalog_old_period_for_parent_code(db_name, parent_code=main_code)


if __name__ == '__main__':
    import os

    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_file_name = os.path.join(db_path, "Normative.sqlite3")

    transfer_raw_data_to_catalog(db_file_name, directory='machines', main_code='2')
