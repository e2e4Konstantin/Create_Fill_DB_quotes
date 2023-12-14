from icecream import ic
from config import dbTolls
from sql_queries import sql_items_queries, sql_catalog_queries


def insert_root_record_to_catalog(db_filename: str) -> int | None:
    """ Вставляем в каталог запись 'Справочник' с шифром '0',
        для обозначения самого верхнего уровня каталога.
        Ссылка на родительскую запись NULL.
    """
    with dbTolls(db_filename) as db:
        code = '0'
        description = 'Справочник расценок'
        item_id = db.get_row_id(sql_items_queries["select_items_id_code"], 'Catalog')

        # ID_parent, period, code, description, FK_tblCatalogs_tblItems
        data = (None, 0, code, description, item_id)
        message = f"вставка корневой записи Каталог' {code}"
        inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
        log = f"добавлена запись: {description!r} id: {inserted_id}"
        ic(log)
        return inserted_id
