import pandas as pd
from icecream import ic
import sqlite3

from config import dbTolls, DirectoryItem
from sql_queries import sql_items_queries, sql_catalog_queries, sql_products_queries, sql_raw_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code


def get_directory_id(db: dbTolls, directory_team: str, item_name: str) -> int | None:
    """ Ищет в таблице справочников справочник directory_team и возвращает id записи item_name. """
    directory_id = db.get_row_id(
        sql_items_queries["select_item_id_team_name"], (directory_team, item_name)
    )
    if directory_id is None:
        output_message_exit(f"в Справочнике {directory_team!r}:", f"не найдена запись {item_name!r}")
        return None
    return directory_id


def get_parent_catalog_id(db: dbTolls, raw_parent_id: int, period: int) -> int | None:
    """ По raw_parent_id родителя из сырой таблицы tblRawData из колонки 'PARENT'.
        Находит его в tblRawData и берет его шифр.
        Ищет в Каталоге запись с таким шифром и периодом возвращает его id
    """
    select = db.go_select(sql_raw_queries["select_rwd_cmt_id"], (raw_parent_id,))
    parent_code = clear_code(select[0]['CMT']) if select else None
    if parent_code:
        return get_catalog_id_by_period_code(db=db, period=period, code=parent_code)
    output_message_exit(f"в таблице tblRawData", f" не найден родитель с 'ID' {raw_parent_id!r}")
    return None


def get_catalog_id_by_period_code(db: dbTolls, period: int, code: str) -> int | None:
    """ Ищет в Каталоге tblCatalogs запись по периоду и шифру. Возвращает id. """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_period_code"], (period, code))
    if row_id:
        return row_id
    output_message(f"В каталоге не найдена запись:", f"период: {period} и шифр: {code!r}")
    return None


def get_catalog_id_by_code(db: dbTolls, code: str) -> int | None:
    """ Ищет в Каталоге tblCatalogs запись по шифру. Возвращает id. """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_code"], (code,))
    if row_id:
        return row_id
    output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return None


def get_catalog_row_by_code(db: dbTolls, code: str) -> sqlite3.Row | None:
    """ Ищет в Каталоге tblCatalogs запись по шифру. Возвращает строку целиком. """
    rows = db.go_select(sql_catalog_queries["select_catalog_code"], (code,))
    if rows:
        return rows[0]
    # output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return None


def get_sorted_directory_items(db_filename: str, directory_name: str) -> tuple[DirectoryItem, ...] | None:
    """ Формирует упорядоченный кортеж (id, name) из элементов справочника directory_name
        в соответствии с иерархией заданной полем ID_parent. Если это поле не задано,
        то берутся все элементы справочника последовательно.
        Первым элементом добавляется элементы группы 'main'.
        """
    # названия групп которые надо отфильтровать, первым вставляется головной элемент
    directory_name = directory_name.lower()
    parameters = ['main', directory_name]
    with (dbTolls(db_filename) as db):
        df = pd.read_sql_query(sql=sql_items_queries["select_items_dual_teams"], params=parameters, con=db.connection)
    if df.empty:
        return None
    columns = ['ID_tblItem', 'ID_parent']
    df[columns] = df[columns].astype(pd.Int8Dtype())
    # Есть только один элемент у которого нет родителя, справочник иерархический
    if df['ID_parent'].isna().sum() == 1:
        dir_tree: list[DirectoryItem] = []
        top_item = df[df['ID_parent'].isna()].to_records(index=False).tolist()[0]
        top_id = top_item[0]
        dir_tree.append(DirectoryItem(
            id=top_id, item_name=top_item[2], directory_name=parameters[0],
            re_pattern=top_item[5], re_prefix=top_item[6])
        )
        item_df = df[df['ID_parent'] == top_id]
        while not item_df.empty:
            item_data = item_df.to_records(index=False).tolist()[0]
            next_id = item_data[0]
            dir_tree.append(DirectoryItem(
                id=next_id, item_name=item_data[2], directory_name=item_data[1],
                re_pattern=item_data[5], re_prefix=item_data[6])
            )
            item_df = df[df['ID_parent'] == next_id]
        return tuple(dir_tree)

    df = df.drop(df.index[0])
    df = df.sort_values(by='name', ascending=True)
    return tuple([DirectoryItem(row[0], row[2], row[1], row[5], row[6]) for row in df.itertuples(index=False)])


def delete_catalog_quotes_with_old_period(db_filename: str):
    """ Поучает максимальный период из всех записей таблицы каталога.
        Обновляет период у головной записи на максимальный.
        Удаляет все записи у которых период < максимального. """
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_catalog_queries["select_catalog_max_period"])
        max_period = work_cursor.fetchone()['max_period'] if work_cursor else None
        if max_period is None:
            output_message_exit(f"при получении максимального периода Каталога", f"{max_period=}")
            return
        ic(max_period)

        db.go_execute(sql_catalog_queries["update_catalog_period_main_row"], (max_period,))
        changes = db.go_execute(sql_catalog_queries["select_changes"]).fetchone()['changes']
        message = f"обновлен период {max_period} головной записи: {changes}"
        ic(message)

        deleted_cursor = db.go_execute(sql_catalog_queries["select_count_last_period"], (max_period,))
        number_past = deleted_cursor.fetchone()[0]
        if number_past > 0:
            mess = f"Из Каталога будут удалены {number_past} записей у которых период меньше текущего: {max_period}"
            ic(mess)
            deleted_cursor = db.go_execute(sql_catalog_queries["delete_catalog_last_periods"], (max_period,))
            mess = f"Из Каталога удалено {deleted_cursor.rowcount} записей с period < {max_period}"
            ic(mess)


def delete_catalog_old_period_for_parent_code(db_filename: str, parent_code: str):
    """
    Удаляет все записи из Каталога у которых период < максимального начиная с родительской записи 'parent_code'.
    Вычисляет максимальный период для всех дочерних записей каталога начиная с родительской записи с шифром parent_code.
    """
    with (dbTolls(db_filename) as db):
        max_period_res = db.go_select(sql_catalog_queries["select_catalog_max_level_period"], (parent_code,))
        if max_period_res is None:
            output_message_exit(f"при получении максимального периода Каталога", f"для {parent_code=}")
            return
        max_period = max_period_res[0]['max_period']
        mess = f"Для записи с кодом: {parent_code!r} максимальный период: {max_period}"
        ic(mess)
        count_cursor = db.go_execute(
            sql_catalog_queries["select_catalog_count_level_period"], (parent_code, max_period)
        )
        number = count_cursor.fetchone()[0] if count_cursor else None
        if number and number > 0:
            mess = (f"Из Каталога для родителя {parent_code} будут удалены {number} записей "
                    f"у которых период меньше: {max_period}")
            ic(mess)
            deleted_cursor = db.go_execute(
                sql_catalog_queries["delete_catalog_level_last_periods"], (parent_code, max_period)
            )
            mess = (f"Из Каталога для родителя {parent_code} удалено {deleted_cursor.rowcount} записей "
                    f"у которых период < {max_period}")
            ic(mess)


def update_product(db: dbTolls, data: tuple) -> int | None:
    """ Получает кортеж с данными продукта для обновления записи в таблице tblProducts. """
    db.go_execute(sql_products_queries["update_product_id"], data)
    count = db.go_execute(sql_products_queries["select_changes"])
    if count:
        return count.fetchone()['changes']
    output_message(f"продукт {data}", f"не обновлен в таблице tblProducts")
    return None


def insert_product(db: dbTolls, data) -> int | None:
    """ Получает кортеж с данными продукта для вставки в таблицу tblProducts. """
    message = f"INSERT tblProducts шифр {data[2]!r} период: {data[1]}"
    inserted_id = db.go_insert(sql_products_queries["insert_product"], data, message)
    if inserted_id:
        return inserted_id
    output_message(f"продукт {data}", f"не добавлен в tblProducts")
    return None


def get_product_row_by_code(db: dbTolls, product_code: str) -> sqlite3.Row | None:
    """ Получает строку из tblProducts у которой шифр равен product_code. """
    products = db.go_select(sql_products_queries["select_products_code"], (product_code,))
    if products:
        return products[0]
    # output_message(f"в tblProducts не найден", f"продукт с шифром: {product_code}")
    return None


def delete_last_period_product_row(db_filename: str, team: str, name: str):
    """ Вычисляет максимальный период для таблицы tblProducts.
        Удаляет все записи у которых период < максимального.  """
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_products_queries["select_products_max_period_team_name"], (team, name))
        max_period = work_cursor.fetchone() if work_cursor else None
        if max_period is None:
            output_message_exit(f"Что то пошло не так при получении максимального периода Продуктов",
                                f"для {team!r} {name!r}")
            return
        current_max_period = max_period['max_period']
        ic(team, name, current_max_period)
        deleted_cursor = db.go_execute(
            sql_products_queries["select_products_count_period_team_name"], (current_max_period, team, name)
        )
        del_number = deleted_cursor.fetchone()['number']
        if del_number > 0:
            message = (f"Будут удалены {del_number} продуктов с периодом меньше: {current_max_period} "
                       f"для {team!r} {name!r}")
            ic(message)

            deleted_cursor = db.go_execute(
                sql_products_queries["delete_products_period_team_name"], (current_max_period, team, name)
            )
            mess = (f"Из Продуктов удалено {deleted_cursor.rowcount} записей с period < {current_max_period}"
                    f" для {team!r} {name!r}")
            ic(mess)


if __name__ == '__main__':
    import os
    from icecream import ic

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    delete_catalog_old_period_for_level(db_name, parent_code='1')
    # delete_last_period_product_row(db_name, team='units', name='material')

    #
    # directory = get_sorted_directory_items(db_name, directory_name='machines')
    # ic(directory)
    #
    # directory = get_sorted_directory_items(db_name, directory_name='units')
    # ic(directory)
    #
    # directory = get_sorted_directory_items(db_name, directory_name='quotes')
    # ic(directory)
