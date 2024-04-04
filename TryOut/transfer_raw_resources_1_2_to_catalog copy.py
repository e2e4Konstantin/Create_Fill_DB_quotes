import sqlite3
from icecream import ic

from config import LocalData, TON_ORIGIN, MAIN_RECORD_CODE
from config import dbTolls,  DirectoryItem
from sql_queries import sql_raw_queries
from files_features import  output_message_exit
from tools.shared.code_tolls import (
    clear_code, title_catalog_extraction, code_to_number,
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
        # print(item.item_name, item.directory_name)
        # print([(x['pressmark'], x['title']) for x in raw_item_data])
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

        added = len(inserted_success)
        updated = len(updated_success)
        bug = len(raw_item_data) - (added + updated)
        message = f"{item.item_name:>12} : добавлено: {added:>5}, обновлено: {updated:>5}, ошибки: {bug:>5}"
        ic(message)

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
    # message = f"Загружаем Каталог Ресурсов в таблицу tblCatalogs: {catalog_name!r} период id: {period_id}"
    # ic(message)

    with dbTolls(db_file) as db:
        # получить идентификатор каталога
        origin_id = get_origin_id(db, origin_name=catalog_name)
        # получить Справочник элементов каталога для главы 1 и 2
        # отсортированный в соответствии с иерархией
        materials_item_catalog = get_sorted_directory_items(db, directory_name="materials")
        machines_item_catalog = get_sorted_directory_items(db, directory_name="machines")

    # загружаем каталог Главы 1
    ic("каталог глава 1")
    pattern_chapter_1 = "(^\s*1\..*)|(^\s*1\s*$)"
    for item in materials_item_catalog[1:]:
        _save_raw_item_catalog_resources(item, db_file, origin_id, period_id, pattern_chapter_1)
    delete_catalog_old_period_for_parent_code(db_file, origin_id, parent_code='1')

    # загружаем каталог Главы 2
    ic("каталог глава 2")
    pattern_chapter_2 = "(^\s*2\..*)|(^\s*2\s*$)"
    for item in machines_item_catalog[1:]:
        _save_raw_item_catalog_resources(item, db_file, origin_id, period_id, pattern_chapter_2)
    delete_catalog_old_period_for_parent_code(db_file, origin_id, parent_code="2")

    # message = f"Загружен каталог: глава 1 и 2 для {catalog_name!r}"
    # ic(message)


if __name__ == "__main__":
    local = LocalData("office")  # office  # home
    db_file: str = local.db_file
    period_id = 72
    transfer_raw_resource_to_catalog(db_file, TON_ORIGIN, period_id)
