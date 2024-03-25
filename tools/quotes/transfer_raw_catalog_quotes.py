import sqlite3
import pandas as pd
from icecream import ic
from config import dbTolls, items_catalog, DirectoryItem, MAIN_RECORD_CODE
from sql_queries import (
    sql_items_queries, sql_raw_queries, sql_catalog_queries
)
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import clear_code, title_catalog_extraction, get_integer_value, code_to_number
from tools.shared.shared_features import (
    get_sorted_directory_items, get_catalog_id_by_origin_code,
    get_catalog_row_by_code, delete_catalog_old_period_for_parent_code,
    get_raw_data_items, update_catalog, insert_raw_catalog, get_origin_id, get_origin_row_by_id, get_period_by_id
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

    #  db, origin_id, period_id, item, row
def _make_data_from_raw_quotes_catalog(
        db: dbTolls, origin_id: int, period_id: int, item: DirectoryItem, raw_catalog_row: sqlite3.Row
    ) -> tuple | None:
    """ Из строки raw_catalog_row таблицы tblRawData с данными для Каталога.
        Выбирает данные, проверяет их, находит в Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # ic(tuple(row))
    # в рабочей таблице Каталога ищем родителя по шифру
    raw_parent_code = raw_catalog_row["parent_pressmark"]
    if raw_parent_code is None:
        # делаем ссылку на корневую запись каталога
        raw_parent_code = MAIN_RECORD_CODE
        parent_id = get_catalog_id_by_origin_code(
            db=db, origin=origin_id, code=raw_parent_code)
    else:
        parent_code = clear_code(raw_parent_code)
        parent_id = get_catalog_id_by_origin_code(db=db, origin=origin_id, code=parent_code)
    if parent_id and str(item.id):
        # period = get_integer_value(raw_catalog_row["PERIOD"])
        code = clear_code(raw_catalog_row["pressmark"])
        description = title_catalog_extraction(raw_catalog_row["title"], item.re_prefix)
        digit_code = code_to_number(code)

        # FK_tblCatalogs_tblOrigins, ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code
        data = (origin_id, parent_id, period_id, code, description, item.id, digit_code)
        # ic(data)
        return data
    else:
        output_message_exit(f"В Каталоге не найдена родительская запись с шифром: {raw_parent_code!r}",
                            f"для {tuple(raw_catalog_row)} ")
    return None


def _save_raw_item_catalog_quotes(
        item: DirectoryItem, db_filename: str, origin_id: int, period_id: int) -> list[tuple[str, str]] | None:
    """ Записывает все значения типа item_name в каталог из RAW таблицы в таблицу каталога.
        Создает ссылки на родителей.
        Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
        Данные записывается только если период больше либо равен периоду текущей записи. """
    inserted_success, updated_success = [], []
    with (dbTolls(db_filename) as db):
        raw_item_data = get_raw_data_items(db, item)
        if not raw_item_data:
            return None
        for row in raw_item_data:
            # raw_code = clear_code(row["pressmark"])
            pure_data = _make_data_from_raw_quotes_catalog(
                db, origin_id, period_id, item, row)
            raw_code = pure_data[3]
            catalog_row = get_catalog_row_by_code(db, origin_id, raw_code)
            if catalog_row:
                per_id = catalog_row['FK_tblCatalogs_tblPeriods']
                per_record = get_period_by_id(db, per_id)
                catalog_period_supplement_num = per_record['supplement_num']

                per_record = get_period_by_id(db, period_id)
                raw_period_supplement_num = per_record['supplement_num']

                if raw_period_supplement_num >= catalog_period_supplement_num:
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
        perg = f"Период id: {period_id}"
        none_log = f"Непонятных записей: {len(raw_item_data) - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log, perg)
    if inserted_success or updated_success:
        inserted_success.extend(updated_success)
        return inserted_success
    return None


def transfer_raw_quotes_to_catalog(db_file_name: str, catalog_name: str, period_id: int):
    """ Заполняет Каталог данными из RAW таблицы каталога расценок.
        Каталог заполняется последовательно, с самого старшего элемента (Глава...).
        В соответствии с иерархией Справочника 'quotes' в таблице tblItems.
        Иерархия задается родителями в классе ItemCatalogDirectory. """
    ic()
    with dbTolls(db_file_name) as db:
        # получить идентификатор каталога
        origin_id = get_origin_id(db, origin_name=catalog_name)
        ic(origin_id)
        # получить Справочник 'quotes' отсортированный в соответствии с иерархией
        dir_catalog = get_sorted_directory_items(db, directory_name='quotes')
        ic(dir_catalog)
    ic("\n")

    ic("Заполняем каталог Расценок:")
    # заполнить и сохранить Главы корневые записи дерева что бы было на кого ссылаться
    chapters = _save_raw_item_catalog_quotes(
        dir_catalog[1], db_file_name, origin_id, period_id)

    # заполнить остальные сущности
    for item in dir_catalog[2:]:
        _save_raw_item_catalog_quotes(item, db_file_name, origin_id, period_id)
    # удалить из Каталога главы вместе с содержимым период которых меньше чем текущий период (последний)
    for chapter in chapters:
        delete_catalog_old_period_for_parent_code(db_file_name, origin=origin_id, parent_code=chapter[0])


if __name__ == '__main__':
    location = "office"  # office  # home
    data_paths: LocalData = get_data_location(location)
    data_paths.db_file, catalog_name = TON_ORIGIN, period_id = period_id

    transfer_raw_quotes_to_catalog(
        data_paths.db_file,
        catalog_name=TON_ORIGIN,
        period_id=period_id)
