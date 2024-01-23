import sqlite3

from icecream import ic
from config import dbTolls, DirectoryItem, PNWC_CATALOG
from sql_queries import sql_items_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, get_integer_value

from tools.shared_features import (
    get_sorted_directory_items, get_catalog_id_by_origin_code, get_catalog_row_by_code,
    get_raw_data_items, update_catalog, insert_raw_catalog, get_origin_id, get_origin_row_by_id
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


def _make_data_from_raw_items_catalog(
        db: dbTolls, origin_id: int, raw_catalog_row: sqlite3.Row, item: DirectoryItem
) -> tuple | None:
    """ Из строки raw_catalog_row таблицы tblRawData с данными для Каталога.
        Выбирает данные, проверяет их, находит в Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # ic(tuple(row))
    # в Каталоге ищем родителя
    raw_parent_code = raw_catalog_row["PARENT_PRESSMARK"]
    if raw_parent_code is None:
        catalog_name = get_origin_row_by_id(db, origin_id)["name"]
        parent_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=catalog_name)
    else:
        parent_code = clear_code(raw_parent_code)
        parent_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=parent_code)
    if parent_id and str(item.id):
        period = get_integer_value(raw_catalog_row["PERIOD"])
        code = clear_code(raw_catalog_row["PRESSMARK"])
        description: str = raw_catalog_row["TITLE"]
        # FK_tblCatalogs_tblOrigins, ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
        data = (origin_id, parent_id, period, code, description, item.id)
        # ic(data)
        return data
    else:
        output_message_exit(f"В Каталоге не найдена родительская запись с шифром: {raw_parent_code!r}",
                            f"для {tuple(raw_catalog_row)} ")
    return None


def _save_raw_item_catalog(db_filename: str, catalog_id: int, item: DirectoryItem,) -> list[tuple[str, str]] | None:
    """ Записывает все значения типа item.item_name в каталог из таблицы с исходными данными tblRawData
        в таблицу каталога tblCatalogs и создает ссылки на родителей.
        Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
        Записываются только те у кого период больше либо равен предыдущему.
    """
    inserted_success, updated_success = [], []
    with (dbTolls(db_filename) as db):
        raw_item_data = get_raw_data_items(db, item)
        if not raw_item_data:
            return None
        for row in raw_item_data:
            raw_code = clear_code(row["PRESSMARK"])
            raw_period = get_integer_value(row["PERIOD"])
            pure_data = _make_data_from_raw_items_catalog(db, catalog_id, row, item)
            catalog_row = get_catalog_row_by_code(db, catalog_id, raw_code)
            if catalog_row:
                row_period = catalog_row['period']
                if raw_period >= row_period:
                    changed_count = update_catalog(db, catalog_row['ID_tblCatalog'], pure_data)
                    if changed_count:
                        updated_success.append((raw_code, item.item_name))
                else:
                    output_message_exit(
                        f"Ошибка загрузки данных в Каталог, записи с шифром: {raw_code!r}",
                        f"текущий период каталога {row_period} больше загружаемого {raw_period}")
            else:
                work_id = insert_raw_catalog(db, pure_data)
                if work_id:
                    inserted_success.append((raw_code, item.item_name))
        alog = f"Для {item.item_name!r}:Всего входящих записей: {len(raw_item_data)}."
        ilog = f"Добавлено {len(inserted_success)}."
        ulog = f"Обновлено {len(updated_success)}."
        none_log = f"Непонятных записей: {len(raw_item_data) - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)
    if inserted_success or updated_success:
        inserted_success.extend(updated_success)
        return inserted_success
    return None


def transfer_raw_pnwc_resources_to_catalog(db_file_name: str, catalog_name: str):
    """ Заполняет Каталог данными по НЦКР Ресурсам из RAW таблицы tblRawData.
        Иерархия задается родителями в классе ItemCatalogDirectory. """
    ic()
    with (dbTolls(db_file_name) as db):
        # получить идентификатор каталога
        origin_id = get_origin_id(db, origin_name=catalog_name)
        ic(origin_id)
        # получить Справочник нцкр материалов отсортированный в соответствии с иерархией
        dir_catalog = get_sorted_directory_items(db, directory_name='pnwc_materials')
    ic("\nЗаполняем каталог НЦКР материалов:")
    ic(dir_catalog)
    for item in dir_catalog[1:]:
        _save_raw_item_catalog(db_file_name, origin_id, item)


if __name__ == '__main__':
    import os
    from read_csv import read_csv_to_raw_table

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
    pom_catalog = os.path.join(data_path, "Каталог_НЦКР_Временный_каталог_Март_2022_Ресурсы_ТСН.csv")

    period = 0
    read_csv_to_raw_table(db_name, pom_catalog, period)
    transfer_raw_pnwc_resources_to_catalog(db_name, catalog_name=PNWC_CATALOG)
