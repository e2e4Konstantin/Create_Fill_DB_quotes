from icecream import ic

from config import dbTolls, items_catalog
from sql_queries import sql_items_creates


def fill_directory_catalog_items(db_file_name: str):
    """ Заполняет справочник элементов каталога. """
    with dbTolls(db_file_name) as db:
        # (code, name)
        message_item = "вставка записи в справочник объектов каталога."
        for item, data in items_catalog.items():
            inserted_id = db.go_insert(
                query=sql_items_creates["insert_item"],
                src_data=(data.team, item, data.name),
                message=message_item
            )
            # ic(inserted_id, data.team, item, data.name)
