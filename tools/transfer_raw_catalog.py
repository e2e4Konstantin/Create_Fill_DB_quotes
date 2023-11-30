import pandas as pd
from icecream import ic
from config import dbTolls, items_catalog
from sql_queries import sql_directory_selects, sql_catalog_insert, sql_raw_data, sql_catalog_select, sql_catalog_insert
from files_features import output_message
from tools.code_tolls import clear_code, title_extraction


def _get_item_id(period: int, code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице Каталога запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_select["select_catalog_period_code"], period, code)
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r} и период: {period}")
    return row_id


def _get_type_id(item_code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице типов объектов нужный тип, возвращаем id """
    id_catalog_items = db.get_row_id(sql_directory_selects["select_directory_name"], item_code)
    if id_catalog_items is None:
        output_message(f"В таблице типов не найден:",f"название: {item_code!r}")
    return id_catalog_items


def _transfer_raw_data_to_catalog(item: tuple[int, str], db_filename: str):
    """ Записывает item_name в каталог из сырой базы в рабочую и создает ссылки на родителя """
    with dbTolls(db_filename) as db:
        item_data = items_catalog[item[1]]
        result = db.go_execute(sql_raw_data["select_rwd_code_regexp"], (item_data.pattern,))
        if result:
            raw_items = result.fetchall()
            success = []

            for row in raw_items:
                # ic(tuple(row))
                code = clear_code(row["PRESSMARK"])
                raw_parent_code = row["PARENT_PRESSMARK"]
                period = int(row["PERIOD"])
                if raw_parent_code is None:
                    id_parent = _get_item_id(period=0, code='0', db=db)
                else:
                    parent_code = clear_code(raw_parent_code)
                    id_parent = _get_item_id(period=period, code=parent_code, db=db)
                # id типа записи
                id_items = str(item[0])
                if id_parent and id_items:
                    description = title_extraction(row["TITLE"], item[1])
                    # ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
                    data = (id_parent, period, code, description, int(item[0]))
                    # ic(data)
                    message = f"INSERT tblCatalog {item[1]!r} шифр {code!r} период: {period}"
                    inserted_id = db.go_insert(sql_catalog_insert["insert_catalog"], data, message)
                    if inserted_id:
                        success.append(inserted_id)
                else:
                    output_message(f"запись {tuple(row)}", f"не добавлена в БД")
            log = f"добавлено {len(success)} записей {item_data.name!r}."
            ic(log)
        else:
            output_message(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                           f"{item_data.name!r}, {item_data.pattern}")


def _get_sorted_directory_items(db_filename: str, directory_name: str):
    dir_tree = []
    with (dbTolls(db_filename) as db):
        df = pd.read_sql_query(sql=sql_directory_selects["select_directory_all_short"], con=db.connection)
        columns = ['ID_tblDirectoryItem', 'ID_parent']
        df[columns] = df[columns].astype(pd.Int8Dtype())
        df['ID_parent'] = df['ID_parent'].fillna(0)

        tmp_df = df.loc[df['code'] == directory_name]
        while not tmp_df.empty:
            t_id = tmp_df.iloc[0]['ID_tblDirectoryItem']
            dir_tree.append((t_id, tmp_df.iloc[0]['code']))
            tmp_df = df.loc[df['ID_parent'] == t_id]
    return tuple(dir_tree[1:])


def transfer_raw_table_data_to_catalog(operating_db: str):
    """ Заполняет каталог данными из таблицы с сырыми данными в рабочую таблицу. """
    ic()
    dir_catalog = _get_sorted_directory_items(operating_db, directory_name='Catalog')
    ic(dir_catalog)

    for item in dir_catalog[:-1]:                           # расценки убираем
        _transfer_raw_data_to_catalog(item, operating_db)

    # table_name = f"tblRawData"
    # operating_db.go_execute(f"DROP TABLE IF EXISTS {table_name};")


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    catalog_items = _get_sorted_directory_items(db_name, directory_name='Catalog')
    ic(catalog_items)
    transfer_raw_table_data_to_catalog(db_name)
