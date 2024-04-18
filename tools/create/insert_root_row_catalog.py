from icecream import ic
from config import dbTolls, MAIN_RECORD_CODE, DEFAULT_RECORD_CODE
from sql_queries import sql_items_queries, sql_catalog_queries
from files_features import output_message_exit
from tools.shared.shared_features import get_origin_id, get_catalog_id_by_origin_code
from tools.shared.code_tolls import code_to_number


def _update_catalog_parent_himself(db: dbTolls, id: int) -> int | None:
    """ Меняет ссылку родителя на самого себя. """
    db.go_execute(sql_catalog_queries["update_catalog_parent_himself"], (id, ))
    count = db.go_execute("""SELECT CHANGES() AS changes;""")
    if count:
        return count.fetchone()['changes']
    output_message_exit(f"элемент каталога {id=}", "не обновлен в таблице tblCatalogs")
    return None


def insert_root_record_to_catalog(db_filename: str, catalog: str, code: str, period: int, description) -> int | None:
    """ Вставляет в каталог запись с шифром code, для обозначения самого верхнего уровня каталога.
        Родительская ссылка указывает на саму запись.
    """
    with dbTolls(db_filename) as db:
        target_item = ('main', 'main')
        item_id = db.get_row_id(sql_items_queries["select_items_id_team_name"], target_item)
        if item_id is None:
            log = f"в справочнике tblItems: не найдена запись {target_item!r}"
            ic(log)
            return None
        origin_id = get_origin_id(db, origin_name=catalog)
        # FK_tblCatalogs_tblOrigins, ID_parent, period, code, description, FK_tblCatalogs_tblItems, digit_code
        data = (origin_id, 1, period, code, description,
                item_id, code_to_number(code))
        message = f"вставка корневой записи в Каталог' {code}"
        inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
        if inserted_id:
            _update_catalog_parent_himself(db, inserted_id)
            log = f"добавлена запись в каталог {catalog}: {description!r} id: {inserted_id}"
            ic(log)
            return inserted_id
    output_message_exit("Не добавлена", f"корневая запись для Каталога {code}")

