from icecream import ic
from config import dbTolls, MAIN_RECORD_CODE
from sql_queries import sql_items_queries, sql_products_queries
from files_features import output_message_exit
from tools.shared.shared_features import get_catalog_id_by_origin_code
from tools.shared.code_tolls import code_to_number



def insert_default_value_record_to_product(
    db: dbTolls, origin_id: int, period: int, default_code: str
) -> int | None:
    """
    Вставляет в tblProduct запись с шифром '0.0-0-0', для значений по умолчанию.
    Родителем корневая запись справочника.
    """
    target_item = ("default", "default")
    item_id = db.get_row_id(
        sql_items_queries["select_items_id_team_name"], target_item
    )
    if item_id is None:
        log = f"в справочнике tblItems: не найдена запись {target_item!r}"
        ic(log)
        return None
    # получаем ссылку на корневую запись каталога
    catalog_id = get_catalog_id_by_origin_code(db, origin_id, code=MAIN_RECORD_CODE)
    description = "Значение по умолчанию"
    data = (
        catalog_id,
        item_id,
        origin_id,
        period,
        default_code,
        description,
        None,
        code_to_number(default_code),
    )
    message = f"Вставка записи {default_code} {description!r} в Продукты"
    inserted_id = db.go_insert(
            sql_products_queries["insert_product"], data, message
    )
    if inserted_id:
        log = f"добавлена запись в продукты {origin_id=}: {description!r} id: {inserted_id}"
        ic(log)
        return inserted_id
    output_message_exit(
        "Не добавлена", f"В Продукты не добавлена запись {description} {default_code}"
    )


if __name__ == "__main__":
    from config import LocalData, TON_ORIGIN

    local = LocalData("office")

    insert_default_value_record_to_product(local.db_file, catalog=TON_ORIGIN, period=0)