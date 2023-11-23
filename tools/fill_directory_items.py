from icecream import ic

from config import dbTolls, src_catalog_items
from sql_queries import sql_catalog


def fill_directory_catalog_items(db_file_name: str):
    """ Создает корневую запись всех справочников.
        Заполняет справочник данными из списка объектов каталога.
        Создается иерархия в соответствии с этим списком.
     """
    with dbTolls(db_file_name) as db:
        root_data = src_catalog_items[0]
        message_root = "вставка корневой записи в справочник объектов каталога."
        message_item = "вставка записи в справочник объектов каталога."
        id_inserted_row = db.try_insert(sql_catalog["insert_root_item_directory"], root_data, message_root)
        ic(id_inserted_row, root_data)
        for data in src_catalog_items[1:]:
            id_inserted_row = db.try_insert(sql_catalog["insert_item_directory"], data, message_item)
            ic(id_inserted_row, data)
