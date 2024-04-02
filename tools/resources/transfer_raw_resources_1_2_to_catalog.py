import sqlite3
from config import LocalData, TON_ORIGIN, MAIN_RECORD_CODE

from icecream import ic
from config import dbTolls, items_catalog, DirectoryItem
from sql_queries import (
    sql_items_queries, sql_raw_queries, sql_catalog_queries, sql_origins
)
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import (
    clear_code,
    title_catalog_extraction,
    get_integer_value,
    code_to_number,
)
from tools.shared.shared_features import (
    get_period_by_id,
    update_catalog,
    insert_raw_catalog,

    get_sorted_directory_items,
    get_catalog_id_by_origin_code,
    delete_catalog_old_period_for_parent_code,
    get_catalog_row_by_code,
    get_origin_id,
    get_origin_row_by_id,
)


def _get_raw_resource_items_chapter(
    db: dbTolls,
    pattern_chapter: str,
    item: DirectoryItem,
) -> list[sqlite3.Row] | None:
    """
    Для ресурсов Выбрать все записи из таблицы tblRawData у которых шифр соответствует паттерну
    для item типа записей.
    !!! В запросе добавляется столбец [parent_pressmark].
    """
    raw_cursor = db.go_execute(
        sql_raw_queries["select_raw_items_by_chapter"], (pattern_chapter, item.re_pattern,)
    )
    results = raw_cursor.fetchall() if raw_cursor else None
    if not results:
        output_message_exit(
            "в RAW таблице с данными для каталога Ресурсов не найдено ни одной записи:",
            f"{item.item_name!r}, {pattern_chapter!r},  {item.re_pattern!r}",
        )
        return None
    return results


# def _get_raw_resource_items(db: dbTolls, item: DirectoryItem) -> list[sqlite3.Row] | None:
#     """
#     Для ресурсов Выбрать все записи из таблицы tblRawData у которых шифр соответствует паттерну
#     для item типа записей.
#     !!! В запросе добавляется столбец [parent_pressmark].
#     """
#     raw_cursor = db.go_execute(
#         sql_raw_queries["select_raw_resource_by_code"], (item.re_pattern,)
#     )
#     results = raw_cursor.fetchall() if raw_cursor else None
#     if not results:
#         output_message_exit(
#             "в RAW таблице с данными для каталога Ресурсов не найдено ни одной записи:",
#             f"{item.item_name!r}, {item.re_pattern}",
#         )
#         return None
#     return results


def _make_data_from_raw_resource_catalog(
    db: dbTolls,
    origin_id: int,
    period_id: int,
    item: DirectoryItem,
    raw_line: sqlite3.Row,
) -> tuple | None:
    """Из строки raw_catalog_row таблицы tblRawData с данными для Каталога.
    Выбирает данные, проверяет их, находит в Каталоге запись родителя.
    Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
    """
    # ic(tuple(row))
    # в рабочей таблице Каталога ищем родителя по шифру
    if raw_line["parent_id"] is None:
        # делаем ссылку на корневую запись каталога
        parent_id = get_catalog_id_by_origin_code(db, origin_id, MAIN_RECORD_CODE )
    else:
        # ищем родителя по шифру в боевой таблице каталога tblCatalogs
        parent_code = clear_code(raw_line["parent_pressmark"])
        parent_id = get_catalog_id_by_origin_code(db, origin_id, parent_code)
    if parent_id and str(item.id):
        code = clear_code(raw_line["pressmark"])
        description = title_catalog_extraction(raw_line["title"], item.re_prefix)
        digit_code = code_to_number(code)

        # FK_tblCatalogs_tblOrigins, ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code
        data = (origin_id, parent_id, period_id, code, description, item.id, digit_code)
        # ic(data)
        return data
    else:
        output_message_exit(
            f"В Каталоге не найдена родительская запись с шифром: {parent_code!r}",
            f"для {tuple(raw_line)} ",
        )
    return None


def _save_raw_item_catalog_resources(
    item: DirectoryItem, db_file: str, origin_id: int, period_id: int, pattern_chapter: str
) -> list[tuple[str, str]] | None:
    """
    Записывает все значения item в каталог из таблицы с исходными данными tblRawData.
    Создает ссылки на родителей.
    Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
    Период записываем только если он больше либо равен предыдущему.
    """
    inserted_success, updated_success = [], []
    with dbTolls(db_file) as db:
        raw_item_data = _get_raw_resource_items_chapter(db, pattern_chapter, item)
        if not raw_item_data:
            return None
        for line in raw_item_data:
            pure_data = _make_data_from_raw_resource_catalog(
                db, origin_id, period_id, item, line
            )
            raw_code = pure_data[3]
            catalog_row = get_catalog_row_by_code(db, origin_id, raw_code)
            if catalog_row:
                per_id = catalog_row["FK_tblCatalogs_tblPeriods"]

                per_record = get_period_by_id(db, per_id)
                catalog_period_supplement_num = per_record["supplement_num"]

                per_record = get_period_by_id(db, period_id)
                raw_period_supplement_num = per_record["supplement_num"]

                if raw_period_supplement_num >= catalog_period_supplement_num:
                    changed_count = update_catalog(
                        db, catalog_row["ID_tblCatalog"], pure_data
                    )
                    if changed_count:
                        updated_success.append((raw_code, item.item_name))
                else:
                    output_message_exit(
                        f"Ошибка загрузки данных в Каталог, записи с шифром: {raw_code!r}",
                        f"номер дополнения записи каталога в БД {catalog_period_supplement_num} больше чем у загружаемой записи {raw_period_supplement_num}",
                    )
            else:
                work_id = insert_raw_catalog(db, pure_data)
                if work_id:
                    inserted_success.append((id, raw_code))
        alog = f"Для {item.item_name!r} всего входящих записей: {len(raw_item_data)}."
        ilog = f"Добавлено {len(inserted_success)}."
        ulog = f"Обновлено {len(updated_success)}."
        perg = f"Период id: {period_id}"
        none_log = f"Непонятных записей: {len(raw_item_data) - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log, perg)
    if inserted_success or updated_success:
        inserted_success.extend(updated_success)
        return inserted_success
    return None




def transfer_raw_resource_to_catalog(
    db_file: str, catalog_name: str, period_id: int
):
    """
    Каталог для глав: 1, 2, 77, 99, 100
    Заполняет Каталог данными из RAW таблицы каталога Ресурсов.
    Каталог заполняется последовательно, с самого старшего элемента (Глава...).
    В соответствии с иерархией Справочника 'quotes' в таблице tblItems.
    Иерархия задается родителями в классе ItemCatalogDirectory.
    """
    with dbTolls(db_file) as db:
        # получить идентификатор каталога
        origin_id = get_origin_id(db, origin_name=catalog_name)
        # получить Справочник элементов каталога для главы 1 и 2
        # отсортированный в соответствии с иерархией
        materials_item_catalog = get_sorted_directory_items(db, directory_name="materials")
        machines_item_catalog = get_sorted_directory_items(db, directory_name="machines")

    # загружаем каталог Главы 1
    pattern_chapter_1 = "(^\s*1\..*)|(^\s*1\s*$)"
    for item in materials_item_catalog[1:]:
        ic(item)
        _save_raw_item_catalog_resources(item, db_file, origin_id, period_id, pattern_chapter_1)
    delete_catalog_old_period_for_parent_code(db_file, origin_id, parent_code='1')

    # загружаем каталог Главы 2
    pattern_chapter_2 = "(^\s*2\..*)|(^\s*2\s*$)"
    for item in machines_item_catalog[1:]:
        ic(item)
        _save_raw_item_catalog_resources(item, db_file, origin_id, period_id, pattern_chapter_2)
    delete_catalog_old_period_for_parent_code(db_file, origin_id, parent_code="2")


if __name__ == "__main__":
    local = LocalData("office")  # office  # home
    db_file: str = local.db_file
    period_id = 67
    transfer_raw_resource_to_catalog(db_file, TON_ORIGIN, period_id)
