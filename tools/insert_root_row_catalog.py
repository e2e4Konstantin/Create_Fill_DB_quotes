from icecream import ic
from config import dbTolls
from sql_queries import sql_directory_selects, sql_catalog_insert_update


def insert_root_record_to_catalog(db_filename: str) -> int | None:
    """ Вставляем в каталог запись 'Справочник' с шифром '0',
        для обозначения самого верхнего уровня каталога.
        Ссылка на родительскую запись NULL.
    """
    with dbTolls(db_filename) as db:
        code = '0'
        root_name = 'Catalog'
        description = 'Справочник расценок'
        query = sql_directory_selects["select_directory_name"]
        id_directory_item = db.get_row_id(query, root_name)
        # period, code, description, FK_tblCatalogs_tblDirectoryItems
        data = (None, 0, code, description, id_directory_item)
        message = f"вставка корневой записи Каталог' {code}"
        inserted_id = db.go_insert(sql_catalog_insert_update["insert_catalog"], data, message)
        log = f"добавлена запись: {description!r} id: {inserted_id}"
        ic(log)
        return inserted_id
