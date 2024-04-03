import sqlite3
from icecream import ic

from files_features import output_message_exit
from config import dbTolls, LocalData, TON_ORIGIN, MAIN_RECORD_CODE
from sql_queries import sql_raw_queries
from tools.shared.shared_features import (
    get_catalog_id_by_origin_code,
    transfer_raw_items,
)
from tools.shared.code_tolls import clear_code, text_cleaning, code_to_number


def _get_raw_resources_like_pressmark(
    db: dbTolls, like_pressmark: str
) -> list[sqlite3.Row] | None:
    """Выбрать записи Ресурсов из таблицы tblRawData по шаблону шифра."""
    results = db.go_select(sql_raw_queries["select_raw_like_pressmark"], (like_pressmark, ))
    if not results:
        output_message_exit(
            "в RAW таблице с Ресурсами нет записей:", f"для шаблона {like_pressmark!r}"
        )
        return None
    return results

def _make_data_from_raw_resources(
    db: dbTolls, origin_id: int, raw_quote: sqlite3.Row, item_id: int, period_id: int
) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с Ресурсом, id типа записи id периода.
    Ищет в Каталоге Родительскую запись по шифру и периоду.
    Выбирает и готовит данные.
    Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    # id                TEXT,
    # pressmark         TEXT,
    # title             TEXT,
    # unit_measure      TEXT,
    # type_resources    TEXT,
    # id_period         TEXT,
    # id_group_resource TEXT,
    # group_pressmark   TEXT


    holder_code = raw_quote["group_pressmark"]
    if holder_code is None:
        # указатель на корневую запись каталога
        holder_id = get_catalog_id_by_origin_code(db, origin_id, MAIN_RECORD_CODE )
    else:
        # в Каталоге!!! ищем родительскую запись с шифром holder_cod
        holder_id = get_catalog_id_by_origin_code(db, origin_id, clear_code(holder_code))
    if holder_id:
        code = clear_code(raw_quote["pressmark"])
        description = text_cleaning(raw_quote["title"]).capitalize()
        measurer = text_cleaning(raw_quote["unit_measure"])
        digit_code = code_to_number(code)

        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
        # FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods,
        # code, description, measurer, digit_code
        data = (
            holder_id,
            item_id,
            origin_id,
            period_id,
            code,
            description,
            measurer,
            digit_code,
        )
        return data
    else:
        output_message_exit(
            f"Для Ресурса {raw_quote['pressmark']!r} в Каталоге не найдена родительская запись",
            f"шифр {holder_code!r}",
        )
    return None



def transfer_raw_resource_to_products(db_file: str, catalog_name: str, period_id: int):
    """
    Заполняет в таблицу tblProducts ресурсами 1 и 2 глава из RAW таблицы tblRawData.
    """
    with dbTolls(db_file) as db:

        # создать индекс в tblRawData
        db.go_execute(sql_raw_queries["delete_index_raw_resources"])
        db.go_execute(sql_raw_queries["create_index_raw_resources"])
        # message = f"Загружаем Ресурсы в таблицу tblProducts: {catalog_name!r} период id: {period_id}"
        # ic(message)
        directory_name = "units"
        # Глава 1
        ic("ресурсы глава 1")
        chapter = '1.%'
        raw_resources_chapter = _get_raw_resources_like_pressmark(db, chapter)
        if raw_resources_chapter is None:
            return None
        item_name = "material"
        transfer_raw_items(
            db, catalog_name, directory_name, item_name,
            _make_data_from_raw_resources, period_id, raw_resources_chapter,
        )
        # Глава 2
        ic("ресурсы глава 2")
        chapter = "2.%"
        raw_resources_chapter = _get_raw_resources_like_pressmark(db, chapter)
        if raw_resources_chapter is None:
            return None
        item_name = "machine"
        transfer_raw_items(
            db, catalog_name, directory_name, item_name,
            _make_data_from_raw_resources, period_id, raw_resources_chapter,
        )
        # message = f"Загружены: глава 1 и 2 для {catalog_name!r} период id: {period_id}"
        # ic(message)


if __name__ == '__main__':
    local = LocalData("office")  # office  # home
    db_file: str = local.db_file
    period_id = 67

    transfer_raw_resource_to_products(db_file, TON_ORIGIN, period_id)
