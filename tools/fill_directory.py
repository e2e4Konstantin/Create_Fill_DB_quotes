from icecream import ic

from config import dbTolls, items_catalog, src_periods_holder
from sql_queries import sql_items_creates, sql_items_queries, sql_origins

from config import TON_CATALOG, PNWC_CATALOG, POM_CATALOG


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
            (TON_CATALOG, 'Территориальные сметные нормативы'),
            (PNWC_CATALOG, 'Нормативы Цен на Комплексы Работ'),
            (POM_CATALOG, 'Проектно Сметные Модули')
        )
        for origin in origin_items:
            inserted_id = db.go_insert(sql_origins['insert_origin'], origin, message_item)


def fill_directory_periods(db_file_name: str) -> int:
    """ Заполняет справочники Периодов. """
    # INSERT INTO tblItems (team, name, title, ID_parent, re_pattern, re_prefix) VALUES ( ?, ?, ?, ?, ?, ?);
    # SELECT ID_parent, start_date, end_date, additive_num, index_num, type, description, holder FROM tblTest;
    # update tblTest set type='Индекс'  where type='Индекс';
    # update tblTest set type='Дополнение'  where type='Дополнение ';

    with dbTolls(db_file_name) as db:
        message_item = "заполнение справочников для таблицы Периодов."
        for item in src_periods_holder:
            inserted_id = db.go_insert(
                query=sql_items_creates["insert_item"], src_data=item, message=message_item
            )
    return 0




if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    # fill_directory_catalog_items(db_name)
    # fill_directory_origins(db_name)
    fill_directory_periods(db_name)
