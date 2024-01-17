from icecream import ic

from config import dbTolls, items_catalog
from sql_queries import sql_items_creates, sql_items_queries, sql_origins


def fill_directory_catalog_items(db_file_name: str):
    """ Заполняет справочник элементов каталога. """
    with dbTolls(db_file_name) as db:
        message_item = "вставка записи в справочник объектов каталога."
        item = items_catalog[0]
        main_data = (item.team, item.name, item.title, None, item.re_pattern, None)
        main_id = db.go_insert(
            query=sql_items_creates["insert_item"], src_data=main_data, message=message_item
        )
        for item in items_catalog[1:]:
            if item.parent is None:
                parent_id = main_id if item.team != 'units' else None
            else:
                parent_id = db.get_row_id(sql_items_queries['select_items_id_team_name'], (item.team, item.parent))

            query_params = (item.team, item.name, item.title, parent_id, item.re_pattern, item.prefix)
            inserted_id = db.go_insert(
                query=sql_items_creates["insert_item"], src_data=query_params, message=message_item
            )
            # ic(inserted_id, item.team, item, item.name, parent_id)


def fill_directory_origins(db_file_name: str):
    """ Заполняет справочник происхождения tblOrigins. """
    with dbTolls(db_file_name) as db:
        message_item = "вставка данных в справочник Происхождения продуктов."
        origin_items = (
            ('ТСН', 'База сметных нормативов'),
            ('НЦКР', 'Нормативы Цен на Комплексы Работ'),
            ('ПСМ', 'Проектно Сметный Модуль')
        )
        for origin in origin_items:
            inserted_id = db.go_insert(sql_origins['insert_origin'], origin, message_item)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    fill_directory_catalog_items(db_name)
    fill_directory_origins(db_name)

