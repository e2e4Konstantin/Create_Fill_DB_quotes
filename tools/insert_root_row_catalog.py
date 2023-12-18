from icecream import ic
from config import dbTolls
from sql_queries import sql_items_queries, sql_catalog_queries


def insert_root_record_to_catalog(db_filename: str) -> int | None:
    """ Вставляем в каталог запись 'Справочник' с шифром '0000',
        для обозначения самого верхнего уровня каталога.
        Ссылка на родительскую запись NULL.
    """
    with dbTolls(db_filename) as db:
        target_item = ('main', 'main')
        item_id = db.get_row_id(sql_items_queries["select_items_team_code"], target_item)
        if item_id is None:
            log = f"НЕ добавлена головная запись в справочник: {target_item!r} id: {item_id}"
            ic(log)
            return None
        # ID_parent, period, code, description, FK_tblCatalogs_tblItems
        code = '0000'
        period = 0
        description = 'Справочник нормативов'
        data = (1, period, code, description, item_id)
        message = f"вставка корневой записи в Каталог' {code}"
        inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
        if inserted_id:
            log = f"добавлена запись в каталог: {description!r} id: {inserted_id}"
            ic(log)
            return inserted_id

