import sqlite3
import pandas as pd
from icecream import ic
from config import dbTolls, items_catalog
from sql_queries import (
    sql_items_queries, sql_raw_queries, sql_catalog_queries
)
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, title_catalog_extraction, get_integer_value


def _get_catalog_id(code: str, db: dbTolls) -> int | None:
    """ Ищет в Каталоге запись по шифру и периоду """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_code"], (code,))
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return row_id


def _get_directory_id(item_name: str, db: dbTolls) -> int | None:
    """ Ищет в справочнике clip запись с именем item_name, возвращает id """
    directory_name = "clip"
    id_catalog_items = db.get_row_id(
        sql_items_queries["select_items_team_code"], (directory_name, item_name,)
    )
    if id_catalog_items is None:
        output_message(f"В {directory_name} справочнике не найден:", f"название: {item_name!r}")
    return id_catalog_items


def _make_data_from_raw_catalog(db: dbTolls, raw_catalog_row: sqlite3.Row, item: tuple[int, str]) -> tuple:
    """ Из строки raw_catalog_row таблицы tblRawData с данными для Каталога.
        Выбирает данные, проверяет их, находит в Каталоге запись родителя.
        Возвращает кортеж с данными для вставки в Рабочую Таблицу Каталога.
        """
    # ic(tuple(row))
    # в Каталоге ищем родителя
    raw_parent_code = raw_catalog_row["PARENT_PRESSMARK"]
    if raw_parent_code is None:
        parent_id = _get_catalog_id(code='0000', db=db)
    else:
        parent_code = clear_code(raw_parent_code)
        parent_id = _get_catalog_id(code=parent_code, db=db)
    # id типа записи каталога
    id_items = str(item[0])
    if parent_id and id_items:
        period = get_integer_value(raw_catalog_row["PERIOD"])
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


def _update_catalog(db: dbTolls, catalog_id: int, raw_table_row: sqlite3.Row, item: tuple[int, str]) -> int | None:
    """ Формирует строку из Сырой таблицы. Изменяет catalog_id запись в таблице Каталога. """
    data = _make_data_from_raw_catalog(db, raw_table_row, item)
    # ID_parent, period, code, description, FK_tblCatalogs_tblItems, ID_tblCatalog, period
    data = (data + (catalog_id, data[1]))

    db.go_execute(sql_catalog_queries["update_catalog_id_period"], data)
    count = db.go_execute(sql_catalog_queries["select_changes"])
    return count.fetchone()['changes'] if count else None


def _insert_raw_catalog(db: dbTolls, raw_table_row: sqlite3.Row, item: tuple[int, str]) -> int | None:
    """ Формирует строку из Сырой таблицы. Вставляет новую запись в таблицу Каталога. """
    data = _make_data_from_raw_catalog(db, raw_table_row, item)
    message = f"INSERT tblCatalog {item[1]!r} шифр {data[2]!r} период: {data[1]!r}"
    inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], data, message)
    if not inserted_id:
        output_message(f"запись {tuple(raw_table_row)}", f"НЕ добавлена в Каталог.")
        return None
    return inserted_id


def _get_raw_data_items(db: dbTolls, item_name: str) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из сырой таблицы у которых шифр соответствует паттерну для этого типа записей. """
    item_pattern = items_catalog[item_name].re_pattern
    if item_pattern is None:
        return None
    raw_cursor = db.go_execute(sql_raw_queries["select_rwd_code_regexp"], (item_pattern,))
    results = raw_cursor.fetchall() if raw_cursor else None
    if not results:
        output_message(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                       f"{items_catalog[item_name].name!r}, {item_pattern}")
        return None
    return results


def _transfer_raw_item_to_catalog(item: tuple[int, str], db_filename: str):
    """ Записывает все значения типа item_name в каталог из таблицы с исходными данными
        в таблицу каталога и создает ссылки на родителей.
        Если запись с таким шифром уже есть в каталоге, то обновляет ее, иначе вставляет новую.
        Период записываем только если он больше либо равен предыдущему.
    """
    with (dbTolls(db_filename) as db):
        raw_data = _get_raw_data_items(db, item[1])
        if not raw_data:
            return None
        inserted_success, updated_success = [], []
        for row_count, row in enumerate(raw_data):
            raw_code = clear_code(row["PRESSMARK"])
            raw_period = get_integer_value(row["PERIOD"])
            catalog_cursor = db.go_execute(
                sql_catalog_queries["select_catalog_id_code"], (raw_code,)
            )
            catalog_row = catalog_cursor.fetchone() if catalog_cursor else None
            if catalog_row:
                row_period = catalog_row['period']
                row_id = catalog_row['ID_tblCatalog']
                if raw_period >= row_period:
                    work_id = _update_catalog(db, row_id, row, item)
                    if work_id:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка загрузки данных в Каталог, записи с шифром: {raw_code!r}",
                        f"текущий период каталога {row_period} больше загружаемого {raw_period}")
            else:
                work_id = _insert_raw_catalog(db, row, item)
                if work_id:
                    inserted_success.append((id, raw_code))
        alog = f"Для {item[1]!r}:Всего пройдено записей в raw таблице: {row_count + 1}."
        ilog = f"Добавлено {len(inserted_success)}."
        ulog = f"Обновлено {len(updated_success)}."
        none_log = f"Непонятных записей: {row_count + 1 - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)


def _delete_last_period_catalog_row(db_filename: str):
    """ Удалить все записи у которых период < максимального.  """
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_catalog_queries["select_catalog_max_period"])
        max_period = work_cursor.fetchone()['max_period'] if work_cursor else None
        if max_period is None:
            output_message_exit(f"при получении максимального периода Каталога", f"{max_period=}")
            return
        ic(max_period)

        db.go_execute(sql_catalog_queries["update_catalog_period_main_row"], (max_period, ))
        changes = db.go_execute(sql_catalog_queries["select_changes"]).fetchone()['changes']
        message = f"обновлен период {max_period} головной записи: {changes}"
        ic(message)

        deleted_cursor = db.go_execute(sql_catalog_queries["select_count_last_period"], (max_period,))
        number_past = deleted_cursor.fetchone()[0]
        mess = f"Из Каталога будут удалены {number_past} записей у которых период меньше текущего: {max_period}"
        ic(mess)
        #
        deleted_cursor = db.go_execute(sql_catalog_queries["delete_catalog_last_periods"], (max_period,))
        mess = f"Из Каталога удалено {deleted_cursor.rowcount} записей с period < {max_period}"
        ic(mess)


def _get_sorted_items(db_filename: str, directory_name: str) -> tuple[tuple[int, str], ...] | None:
    """ Формирует упорядоченный кортеж из элементов справочник (id, code)
        в соответствии с иерархией заданной полем ID_parent. Если это поле не задано,
        то берутся все элементы справочника последовательно """
    with (dbTolls(db_filename) as db):
        df = pd.read_sql_query(
            sql=sql_items_queries["select_items_team"], params=[directory_name.lower()], con=db.connection
        )
    if df.empty:
        return None
    columns = ['ID_tblItem', 'ID_parent']
    df[columns] = df[columns].astype(pd.Int8Dtype())
    if df['ID_parent'].isna().sum() == 1:
        # справочник иерархический
        # x=df[df['ID_parent'].isna()]['ID_tblItem'].values[0]
        dir_tree = []
        top_item = df[df['ID_parent'].isna()].to_records(index=False).tolist()[0]
        dir_tree.append(top_item[:2])
        top_id = top_item[0]
        item_df = df[df['ID_parent'] == top_id]
        while not item_df.empty:
            item_data = item_df.to_records(index=False).tolist()[0]
            dir_tree.append(item_data[:2])
            next_id = item_data[0]
            item_df = df[df['ID_parent'] == next_id]
        return tuple(dir_tree)

    dir_tree = [(row['ID_tblItem'], row['code']) for row in df.rows]
    return tuple(dir_tree)


def transfer_raw_table_data_to_catalog(operating_db: str):
    """ Заполняет каталог данными из таблицы с исходными данными в таблицу Каталога.
        Каталог заполняется в соответствии с иерархией элементов каталога.
        Иерархия задается родителями в классе ItemCatalogDirectory.
    """
    ic()
    # получить отсортированные по иерархии элементы справочника 'clip'
    dir_catalog = _get_sorted_items(operating_db, directory_name='clip')
    ic(dir_catalog)

    for item in dir_catalog[1:]:
        _transfer_raw_item_to_catalog(item, operating_db)

    # удалить из Каталога записи период которых меньше чем текущий период
    _delete_last_period_catalog_row(operating_db)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    transfer_raw_table_data_to_catalog(db_name)

    # catalog_items = _get_sorted_directory_items(db_name, directory_name='Catalog')
    # ic(catalog_items)
    # (1, 67, '3', 'Строительные работы', 3, 2))

    # transfer_raw_table_data_to_catalog(db_name)

    # _delete_last_period_catalog_row(db_name)
