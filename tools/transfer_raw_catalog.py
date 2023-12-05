import sqlite3
import pandas as pd
from icecream import ic
from config import dbTolls, items_catalog
from sql_queries import (
    sql_directory_selects, sql_catalog_insert_update, sql_raw_data, sql_catalog_select, sql_catalog_delete
)
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, title_catalog_extraction, get_integer_value


def _get_item_id(code: str, db: dbTolls) -> int | None:
    """ Ищем в Каталоге запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_select["select_catalog_id_code"], code)
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return row_id


def _get_type_id(item_code: str, db: dbTolls) -> int | None:
    """ Ищем в таблице типов объектов нужный тип, возвращаем id """
    id_catalog_items = db.get_row_id(sql_directory_selects["select_directory_name"], item_code)
    if id_catalog_items is None:
        output_message(f"В таблице типов не найден:", f"название: {item_code!r}")
    return id_catalog_items


def _make_data_from_raw_catalog(db: dbTolls, raw_catalog_row: sqlite3.Row, item: tuple[int, str]) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированными данными Каталога.
        Выбирает нужные данные, проверяет их, находит в raw Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # ic(tuple(row))
    period = get_integer_value(raw_catalog_row["PERIOD"])
    raw_parent_code = raw_catalog_row["PARENT_PRESSMARK"]
    if raw_parent_code is None:
        parent_id = _get_item_id(code='0', db=db)
    else:
        parent_code = clear_code(raw_parent_code)
        parent_id = _get_item_id(code=parent_code, db=db)
    # id типа записи каталога
    id_items = str(item[0])
    if parent_id and id_items:
        code = clear_code(raw_catalog_row["PRESSMARK"])
        description = title_catalog_extraction(raw_catalog_row["TITLE"], item[1])
        # ID_parent, period, code, description, FK_tblCatalogs_tblDirectoryItems
        data = (parent_id, period, code, description, int(item[0]))
        # ic(data)
        return data
    else:
        output_message_exit(f"В Каталоге не найдена родительская запись с шифром: {raw_parent_code!r}",
                            f"для {tuple(raw_catalog_row)} ")
    return tuple()


def _update_catalog(db: dbTolls, quote_id: int, raw_catalog_row: sqlite3.Row, item: tuple[int, str]) -> int | None:
    """ Получает строку из Сырой таблицы с элементами Каталога. Изменяет запись в таблице Каталога. """
    data = _make_data_from_raw_catalog(db, raw_catalog_row, item) + (quote_id,)
    db.go_execute(sql_catalog_insert_update["update_catalog_id"], data)
    return db.go_execute("""SELECT CHANGES();""")


def _insert_raw_catalog(db: dbTolls, raw_catalog_row: sqlite3.Row, item: tuple[int, str]) -> int | None:
    data = _make_data_from_raw_catalog(db, raw_catalog_row, item)
    message = f"INSERT tblCatalog {item[1]!r} шифр {data[2]!r} период: {data[1]!r}"
    inserted_id = db.go_insert(sql_catalog_insert_update["insert_catalog"], data, message)
    if not inserted_id:
        output_message(f"запись {tuple(raw_catalog_row)}", f"НЕ добавлена в Каталог.")
        return None
    return inserted_id


def _transfer_raw_data_to_catalog(item: tuple[int, str], db_filename: str):
    """ Записывает item_name в каталог из сырой базы в рабочую и создает ссылки на родителя """
    with (dbTolls(db_filename) as db):
        item_data = items_catalog[item[1]]
        raw_cursor = db.go_execute(sql_raw_data["select_rwd_code_regexp"], (item_data.pattern,))
        if raw_cursor:
            inserted_success = []
            updated_success = []

            for raw_count, row in enumerate(raw_cursor.fetchall()):
                raw_code = clear_code(row["PRESSMARK"])
                raw_period = get_integer_value(row["PERIOD"])
                work_cursor = db.go_execute(sql_catalog_select["select_catalog_row_code"], (raw_code,))
                work_row = work_cursor.fetchone() if work_cursor else None
                if work_row:
                    work_period = work_row['period']
                    work_id = work_row['ID_tblCatalog']
                    if raw_period >= work_period:
                        work_id = _update_catalog(db, work_id, row, item)
                        if work_id:
                            updated_success.append((id, raw_code))
                    else:
                        output_message_exit(
                            f"Ошибка загрузки данных в Каталог, записи с шифром: {raw_code!r}",
                            f"текущий период каталога {work_period} старше загружаемого {raw_period}")
                else:
                    work_id = _insert_raw_catalog(db, row, item)
                    if work_id:
                        inserted_success.append((id, raw_code))
            alog = f"Всего пройдено записей в raw таблице: {raw_count + 1}."
            ilog = f"Добавлено {len(inserted_success)} записей {item[1]!r}."
            ulog = f"Обновлено {len(updated_success)} записей {item[1]!r}."
            none_log = f"Непонятных записей: {raw_count + 1 - (len(updated_success) + len(inserted_success))}."
            ic(alog, ilog, ulog, none_log)
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


def _delete_last_period_catalog_row(db_filename: str):
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_catalog_select["select_catalog_max_period"])
        max_period = work_cursor.fetchone() if work_cursor else None
        if max_period:
            current_period = max_period['max_period']
            ic(current_period)
            deleted_cursor = db.go_execute("SELECT COUNT(*) FROM tblCatalogs WHERE period > 0 AND period < ?", (current_period,))
            mess = f"Из Каталога будут удалены {deleted_cursor.fetchone()[0]} записей у которых период меньше текущего: {current_period}"
            ic(mess)
            deleted_cursor = db.go_execute(sql_catalog_delete["delete_catalog_last_periods"], (current_period, ))
            mess = f"Из Каталога удалено {deleted_cursor.rowcount} записей с period < {current_period}"
            ic(mess)
        else:
            output_message_exit(f"Что то пошло не так при получении максимального периода Каталога:",
                                f"{sql_catalog_select['select_max_period']!r}")


def transfer_raw_table_data_to_catalog(operating_db: str):
    """ Заполняет каталог данными из таблицы с сырыми данными в рабочую таблицу.
        Каталог заполняется по иерархии названий элементов каталога.
        Иерархия задается родителями в классе ItemCatalogDirectory.
    """
    ic()
    dir_catalog = _get_sorted_directory_items(operating_db, directory_name='Catalog')
    ic(dir_catalog)

    for item in dir_catalog[:-1]:  # расценки убираем
        _transfer_raw_data_to_catalog(item, operating_db)

    # удалить из Каталога записи период которых меньше чем текущий период
    _delete_last_period_catalog_row(operating_db)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    # catalog_items = _get_sorted_directory_items(db_name, directory_name='Catalog')
    # ic(catalog_items)
    # (1, 67, '3', 'Строительные работы', 3, 2))

    # transfer_raw_table_data_to_catalog(db_name)

    _delete_last_period_catalog_row(db_name)

