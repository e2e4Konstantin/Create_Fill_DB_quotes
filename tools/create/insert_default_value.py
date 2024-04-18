from icecream import ic
from config import dbTolls, MAIN_RECORD_CODE, DEFAULT_RECORD_CODE
from sql_queries import sql_items_queries, sql_products_queries
from files_features import output_message_exit
from tools.shared.shared_features import get_origin_id, get_catalog_id_by_origin_code
from tools.shared.code_tolls import code_to_number, clear_code


def insert_default_value_record_to_product(
    db_filename: str, catalog: str, period: int
) -> int | None:
    """
    Вставляет в tblProduct запись с шифром '0.0-0-0', для значений по умолчанию.
    Родителем корневая запись справочника.
    """
    with dbTolls(db_filename) as db:
        target_item = ("default", "default")
        item_id = db.get_row_id(
            sql_items_queries["select_items_id_team_name"], target_item
        )
        if item_id is None:
            log = f"в справочнике tblItems: не найдена запись {target_item!r}"
            ic(log)
            return None
        origin_id = get_origin_id(db, origin_name=catalog)
        # получаем ссылку на корневую запись каталога
        catalog_id = get_catalog_id_by_origin_code(db, origin_id, code=MAIN_RECORD_CODE)
        description = "Значение по умолчанию"
        code = clear_code(DEFAULT_RECORD_CODE)

        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
        # FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods,
        #  code, description, measurer, digit_code,
        data = (
            catalog_id,
            item_id,
            origin_id,
            period,
            code,
            description,
            None,
            code_to_number(code),
        )
        message = f"Вставка записи '0.0-0-0' {description!r} в Продукты"
        inserted_id = db.go_insert(
            sql_products_queries["insert_product"], data, message
        )
        if inserted_id:
            log = f"добавлена запись в продукты {catalog}: {description!r} id: {inserted_id}"
            ic(log)
            return inserted_id
    output_message_exit(
        "Не добавлена", f"В Продукты не добавлена запись {description} {code}"
    )


if __name__ == "__main__":
    from config import LocalData, TON_ORIGIN

    local = LocalData("office")

    insert_default_value_record_to_product(local.db_file, catalog=TON_ORIGIN, period=0)