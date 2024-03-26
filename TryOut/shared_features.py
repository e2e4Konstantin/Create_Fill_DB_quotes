import pandas as pd
from icecream import ic
import sqlite3

from config import dbTolls, DirectoryItem
from sql_queries import sql_items_queries, sql_catalog_queries, sql_products_queries, sql_raw_queries, sql_origins, sql_periods_queries
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import clear_code


def update_catalog(db: dbTolls, catalog_id: int, raw_catalog_data: tuple) -> int | None:
    """ Изменяет данные в каталоге на новые raw_catalog_data для записи с catalog_id. """

    # FK_tblCatalogs_tblOrigins, ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code
    data = (raw_catalog_data + (catalog_id,))
    db.go_execute(sql_catalog_queries["update_catalog_id"], data)

    count = db.go_execute(sql_catalog_queries["select_changes"])
    return count.fetchone()['changes'] if count else None


def insert_raw_catalog(db: dbTolls, raw_catalog_data: tuple) -> int | None:
    """ Вставляет новую запись в таблицу Каталога. """
    message = f"INSERT tblCatalog {raw_catalog_data}"
    inserted_id = db.go_insert(sql_catalog_queries["insert_catalog"], raw_catalog_data, message)
    if not inserted_id:
        output_message(f"запись {raw_catalog_data}", f"НЕ добавлена в Каталог.")
        return None
    return inserted_id


def get_raw_data_items(db: dbTolls, item: DirectoryItem) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из таблицы tblRawData у которых
        шифр соответствует паттерну для item типа записей.
        Добавляет столбец [parent_pressmark].

    """
    raw_cursor = db.go_execute(sql_raw_queries["select_rwd_code_regexp"], (item.re_pattern,))
    results = raw_cursor.fetchall() if raw_cursor else None
    if not results:
        output_message_exit(f"в RAW таблице с данными для каталога не найдено ни одной записи:",
                            f"{item.item_name!r}, {item.re_pattern}")
        return None
    return results


def get_directory_id(db: dbTolls, directory_team: str, item_name: str) -> int | None:
    """ Ищет в таблице справочников справочник directory_team и возвращает id записи item_name. """
    directory_id = db.get_row_id(
        sql_items_queries["select_item_id_team_name"], (directory_team, item_name)
    )
    if directory_id is None:
        output_message_exit(f"в Справочнике {directory_team!r}:", f"не найдена запись {item_name!r}")
        return None
    return directory_id


def get_parent_catalog_id(db: dbTolls, origin_id: int, raw_parent_id: int) -> int | None:
    """ По raw_parent_id родителя из сырой таблицы tblRawData из колонки 'PARENT'.
        Находит его в tblRawData и берет его шифр.
        Ищет в Каталоге запись с таким шифром и периодом возвращает его id
    """
    select = db.go_select(sql_raw_queries["select_rwd_cmt_id"], (raw_parent_id,))
    parent_code = clear_code(select[0]['CMT']) if select else None
    if parent_code:
        return get_catalog_id_by_origin_code(db=db, origin=origin_id, code=parent_code)
    output_message_exit(f"в каталоге", f"не найден родитель с шифром {parent_code!r}")
    return None


# def get_catalog_id_by_period_code(db: dbTolls, period: int, code: str) -> int | None:
#     """ Ищет в Каталоге tblCatalogs запись по периоду и шифру. Возвращает id. """
#     row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_period_code"], (period, code))
#     if row_id:
#         return row_id
#     output_message(f"В каталоге не найдена запись:", f"период: {period} и шифр: {code!r}")
#     return None


def get_catalog_id_by_origin_code(db: dbTolls, origin: int, code: str) -> int | None:
    """ Ищет в Каталоге tblCatalogs запись по id каталога и шифру. Возвращает id. """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_code"], (origin, code))
    if row_id:
        return row_id
    output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return None


def get_catalog_row_by_code(db: dbTolls, origins_id: int, code: str) -> sqlite3.Row | None:
    """ Ищет в Каталоге tblCatalogs запись по источнику и шифру. Возвращает первую строку. """
    rows = db.go_select(sql_catalog_queries["select_catalog_code"], (origins_id, code,))
    if rows:
        return rows[0]
    # output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return None


def get_sorted_directory_items(db: dbTolls, directory_name: str) -> tuple[DirectoryItem, ...] | None:
    """ Формирует упорядоченный кортеж (id, name) из элементов справочника directory_name
        в соответствии с иерархией заданной полем ID_parent. Если это поле не задано,
        то берутся все элементы справочника последовательно.
        Первым элементом добавляется элементы группы 'main'.
        """
    # названия групп которые надо отфильтровать, первым вставляется головной элемент
    directory_name = directory_name.lower()
    parameters = ['main', directory_name]
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
    """ Удаляет все записи у которых период < максимального.
    Получает максимальный период из всех записей таблицы каталога.
    Обновляет период у головной записи на максимальный. """
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


def delete_catalog_old_period_for_parent_code(db_filename: str, origin: int, parent_code: str):
    """
    Удаляет записи каталога у которых шифр родителя parent_code, тип каталога origin,
    и номер дополнения периода меньше чем максимальный период для всех дочерних записей родительской записи.
    """
    with (dbTolls(db_filename) as db):
        # mess = f"Удаление прошлых периодов для id каталога {origin=} и шифра {parent_code=}"
        # ic(mess)
        query = sql_catalog_queries["select_catalog_max_supplement_period"]
        result = db.go_select(query, (origin, parent_code,))
        if result is None:
            output_message_exit(f"Ошибка при получении максимального периода Каталога",
                                f"для id каталога {origin=} и шифра {parent_code=}")
            return
        max_supplement_periods = result[0]['max_supplement_periods']
        # mess = f"Для записи с кодом: {parent_code!r} максимальный период: {result}"
        # ic(mess)
        count_cursor = db.go_execute(
            sql_catalog_queries["select_catalog_count_period_supplement"],
            (origin, parent_code, max_supplement_periods)
            )
        number = count_cursor.fetchone()[0] if count_cursor else None
        if number and number > 0:
            # mess = (f"Из Каталога для родителя {parent_code} будут удалены {number} записей "
            #         f"у которых номер дополнения периода меньше: {max_supplement_periods}")
            # ic(mess)
            deleted_cursor = db.go_execute(
                sql_catalog_queries["delete_catalog_less_than_specified_supplement_period"],
                (origin, parent_code, max_supplement_periods)
                )
            mess = (f"Из Каталога для родителя {parent_code=} удалено {deleted_cursor.rowcount} записей "
                    f"у которых номер дополнения периода < {max_supplement_periods}")
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


def get_product_all_catalog_by_code(db: dbTolls, product_code: str) -> sqlite3.Row | None:
    """ Получает Запись из tblProducts по шифру. """
    products = db.go_select(sql_products_queries["select_product_all_code"], (product_code,))
    if products:
        return products[0]
    return None


def get_product_by_code(db: dbTolls, origin_id: int, product_code: str) -> sqlite3.Row | None:
    """ Получает строку из tblProducts у которой каталог и шифр равен параметрам. """
    products = db.go_select(sql_products_queries["select_products_origin_code"], (origin_id, product_code))
    if products:
        return products[0]
    # output_message(f"в tblProducts не найден", f"продукт с шифром: {product_code}")
    return None


def get_period_by_title(db: dbTolls, title: str) -> sqlite3.Row | None:
    """ Получает строку из tblPeriods у которой название совпадает с title. """
    period = db.go_select(sql_periods_queries["select_period_by_title"], (title, ))
    if period:
        return period[0]
    return None

def get_period_by_id(db: dbTolls, period_id: int) -> sqlite3.Row | None:
    """ Получает строку из tblPeriods по id. """
    period = db.go_select(
        sql_periods_queries["select_period_by_id"], (period_id, ))
    if period:
        return period[0]
    return None


def delete_last_period_product_row(db: dbTolls, origin_id: int, team: str, name: str):
    """ Удаляет все записи у которых период < максимального. Вычисляет максимальный период для таблицы tblProducts. """
    query_info = f"для {origin_id=!r} {team=!r} {name=!r}"
    work_cursor = db.go_execute(
        sql_products_queries["select_products_max_period_origin_team_name"], (origin_id, team, name)
    )
    max_period = work_cursor.fetchone() if work_cursor else None
    if max_period is None:
        output_message_exit(f"Ошибка при получении максимального периода Продуктов", query_info)
        return
    current_max_period = max_period['max_period']
    ic(origin_id, team, name, current_max_period)
    deleted_count_cursor = db.go_execute(
        sql_products_queries["select_products_count_origin_team_name_period"],
        (origin_id, team, name, current_max_period)
    )
    del_number = deleted_count_cursor.fetchone()['number']
    if del_number > 0:
        message = f"Будут удалены {del_number} продуктов с периодом меньше: {current_max_period} {query_info}"
        ic(message)
        deleted_cursor = db.go_execute(
            sql_products_queries["delete_products_period_team_name"],
            (origin_id, team, name, current_max_period)
        )
        message = f"Из Продуктов удалено {deleted_cursor.rowcount} записей с period < {current_max_period} {query_info}"
        ic(message)


def get_raw_data(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из сырой таблицы. """
    rows = db.go_select(sql_raw_queries["select_rwd_all"])
    if not rows:
        output_message_exit(f"в RAW таблице не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return rows


def get_origin_id(db: dbTolls, origin_name: str) -> int | None:
    """ Получает Id элемента справочника происхождения с именем origin_name. """
    if origin_name is None:
        return None
    origin_id = db.get_row_id(sql_origins["select_origin_name"], (origin_name,))
    if origin_id:
        return origin_id
    output_message_exit(f"в справочнике происхождения tblOrigins:",
                        f"не найдено записи с названием: {origin_name}.")


def get_origin_row_by_id(db: dbTolls, origin_id: int) -> sqlite3.Row | None:
    """ Получает запись элемента происхождения (учета данных)  с указанным id. """
    if origin_id is None or origin_id == 0:
        return None
    origin_row = db.go_select(sql_origins["select_origin_id"], (origin_id,))
    if origin_row:
        return origin_row[0]
    output_message_exit(f"в справочнике происхождения tblOrigins:",
                        f"не найдено записи с id: {origin_id}.")



def transfer_raw_items(
        db: dbTolls, catalog_name: str, directory_name: str, item_name: str,
        get_line_data: callable, period_id: int, raw_items: list[sqlite3.Row] = None
) -> None:
    """ Вставляет или обновляет записи типа item_name в таблицу tblProducts
        В таблице tblProducts ищется продукт с таким же шифром, если такой продукт уже есть в таблице,
        то он обновляется, если нет, то вставляется новый продукт.
    :param db: Курсор БД
    :param catalog_name: название каталога
    :param directory_name: название справочника
    :param item_name: названия элементов, которые обрабатываются (расценка, машина...).
    :param get_line_data: Функция, которая готовит данные для вставки/обновления.
    :param raw_items: Список raw строк БД
    """
    # получить идентификатор каталога
    ic()
    ic(catalog_name, item_name)
    origin_id = get_origin_id(db, origin_name=catalog_name)
    item_id = get_directory_id(db, directory_team=directory_name, item_name=item_name)

    inserted_success, updated_success = [], []
    for db_row in raw_items:
        data = get_line_data(db, origin_id, db_row, item_id, period_id)

        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
        # FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods
        # code, description, measurer, full_code

        raw_code, raw_period = data[4], data[3]

        product = get_product_by_code(db=db, origin_id=origin_id, product_code=raw_code)


        if product:
            if raw_period >= product['period'] and item_id == product['FK_tblProducts_tblItems']:
                count_updated = update_product(db, data + (product['ID_tblProduct'],))
                if count_updated:
                    updated_success.append((id, raw_code))
            else:
                output_message_exit(
                    f"Ошибка обновления Продукта: {raw_code!r} или item_type не совпадает {item_id=!r}",
                    f"период Продукта {product['period']} старше загружаемого {raw_period}")
        else:
            inserted_id = insert_product(db, data)
            if inserted_id:
                inserted_success.append((id, raw_code))
    row_count = len(raw_items)
    alog = f"Всего записей в raw таблице: {row_count}."
    ilog = f"Добавлено {len(inserted_success)} {item_name}."
    ulog = f"Обновлено {len(updated_success)} {item_name}."
    none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
    ic(alog, ilog, ulog, none_log)
    # удалить item записи период которых меньше чем максимальный период
    delete_last_period_product_row(db, origin_id=origin_id, team=directory_name, name=item_name)


if __name__ == '__main__':
    import os
    from icecream import ic

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    # delete_catalog_old_period_for_level(db_name, parent_code='1')
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
