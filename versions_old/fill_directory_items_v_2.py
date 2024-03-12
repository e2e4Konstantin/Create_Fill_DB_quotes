from icecream import ic

from config import dbTolls, items_catalog
from sql_queries import sql_items_creates, sql_items_queries


def fill_directory_catalog_items(db_file_name: str):
    """ Заполняет справочник элементов каталога. """
    with dbTolls(db_file_name) as db:
        # (team, code, name, ID_parent)
        message_item = "вставка записи в справочник объектов каталога."
        for item, data in items_catalog.items():
            if data.parent is None:
                parent_id = None
            else:
                parent_id = db.get_row_id(sql_items_queries['select_items_team_code'], (data.team, data.parent))

            query_params = (data.team, item, data.name, parent_id)
            inserted_id = db.go_insert(
                query=sql_items_creates["insert_item"], src_data=query_params, message=message_item
            )
            # ic(inserted_id, data.team, item, data.name, parent_id)
