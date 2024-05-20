import pandas as pd
from icecream import ic
import sqlite3

from config import dbTolls, DirectoryItem, DEFAULT_RECORD_CODE, MAIN_RECORD_CODE, Period

from sql_queries import sql_items_queries, sql_catalog_queries, sql_products_queries, sql_raw_queries, sql_origins, sql_periods_queries
from files_features import output_message, output_message_exit
from tools.shared.code_tolls import clear_code, code_to_number
from files_features import output_message_exit




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
        output_message(f"запись {raw_catalog_data}", "НЕ добавлена в Каталог.")
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
        output_message_exit("в RAW таблице с данными для каталога не найдено ни одной записи:",
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
    output_message_exit("в каталоге", f"не найден родитель с шифром {parent_code!r}")
    return None


# def get_catalog_id_by_period_code(db: dbTolls, period: int, code: str) -> int | None:
#     """ Ищет в Каталоге tblCatalogs запись по периоду и шифру. Возвращает id. """
#     row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_period_code"], (period, code))
#     if row_id:
#         return row_id
#     output_message(f"В каталоге не найдена запись:", f"период: {period} и шифр: {code!r}")
#     return None


def get_catalog_id_by_origin_code(db: dbTolls, origin: int, code: str) -> int | None:
    """
    Ищет запись в таблице 'tblCatalogs', используя идентификатор и код каталога.
    Если запись найдена, она возвращает идентификатор, в противном случае возвращает None.
    """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_code"], (origin, code))
    if row_id:
        return row_id
    output_message("В каталоге не найдена запись:", f"шифр: {code!r}")
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
            output_message_exit("при получении максимального периода Каталога", f"{max_period=}")
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
            output_message_exit("Ошибка при получении максимального периода Каталога",
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
            mess = (
                f"для шифра '{parent_code:<5}' удалено {deleted_cursor.rowcount:<5} записей "
                f"номер дополнения < {max_supplement_periods}"
            )
            ic(mess)


def update_product(db: dbTolls, data: tuple) -> int | None:
    """ Получает кортеж с данными продукта для обновления записи в таблице tblProducts. """
    db.go_execute(sql_products_queries["update_product_id"], data)
    count = db.go_execute(sql_products_queries["select_changes"])
    if count:
        return count.fetchone()['changes']
    output_message(f"продукт {data}", "не обновлен в таблице tblProducts")
    return None


def insert_product(db: dbTolls, data) -> int | None:
    """ Получает кортеж с данными продукта для вставки в таблицу tblProducts. """
    message = f"INSERT tblProducts шифр {data[4]!r} период id: {data[3]}"
    inserted_id = db.go_insert(sql_products_queries["insert_product"], data, message)
    if inserted_id:
        return inserted_id
    output_message(f"продукт {data}", "не добавлен в tblProducts")
    return None


def get_product_all_catalog_by_code(db: dbTolls, product_code: str) -> sqlite3.Row | None:
    """ Получает Запись из tblProducts по шифру. """
    products = db.go_select(sql_products_queries["select_product_all_code"], (product_code,))
    if products:
        return products[0]
    return None


def get_product_by_code(db: dbTolls, origin_id: int, product_code: str) -> sqlite3.Row | None:
    """ Получает строку из tblProducts по каталогу и шифру равен параметрам. """
    products = db.go_select(sql_products_queries["select_products_origin_code"], (origin_id, product_code))
    if products:
        return products[0]
    # message = f"в tblProducts не найдена запись с шифром: {product_code}")
    return None

def get_history_product_by_code(db: dbTolls, origin_id: int, product_code: str) -> sqlite3.Row | None:
    """Получает 1 строку из _tblHistoryProducts по каталогу и шифру."""
    products = db.go_select(
        sql_products_queries["select_history_products_origin_code"],
        (origin_id, product_code),
    )
    if products:
        return products[0]
    # message = f"в tblProducts не найдена запись с шифром: {product_code}")
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

def get_period_by_origin_and_numbers(db: dbTolls, origin_id: int, period: Period) -> sqlite3.Row | None:
    """Получает строку из tblPeriods по справочнику и номерам."""
    period_result = db.go_select(
        sql_periods_queries["select_period_by_origin_and_numbers"],
        (origin_id, period.supplement, period.index)
    )
    if period_result:
        return period_result[0]
    return None


def delete_last_period_product_row(db: dbTolls, origin_id: int, item_id: int):
    """ Удаляет все записи у которых период < максимального.
    Вычисляет максимальный период для таблицы tblProducts. """

    query_info = f"для {origin_id=!r} {item_id=!r}"
    work_cursor = db.go_execute(
        sql_products_queries["select_products_max_supplement_origin_item"], (origin_id, item_id)
    )
    result = work_cursor.fetchone() if work_cursor else None
    if result is None:
        output_message_exit("Ошибка при получении максимального Номера дополнения периода Продуктов", query_info)
        return
    max_supplement_number = result['max_suppl']
    # ic(origin_id, item_id, max_supplement_number)
    count_records_to_be_deleted = db.go_execute(
        sql_products_queries["select_products_count_origin_item_less_supplement"],
        (origin_id, item_id, max_supplement_number)
    )
    number = count_records_to_be_deleted.fetchone()['number']
    if number > 0:
        # message = f"Будут удалены {number} продуктов с периодом меньше: {max_supplement_number} {query_info}"
        # ic(message)
        deleted_cursor = db.go_execute(
            sql_products_queries["delete_products_origin_item_less_max_supplement"],
            (origin_id, item_id, max_supplement_number)
        )
        message = f"удалено {deleted_cursor.rowcount} записей с период дополнения < {max_supplement_number}"
        ic(message)


def get_raw_data(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все записи из сырой таблицы. """
    try:
        rows = db.go_select(sql_raw_queries["select_rwd_all"])
    except AttributeError as e:
        output_message_exit("Ошибка при получении данных из RAW таблицы:",
                            repr(e))
        return None
    if rows is None:
        output_message_exit("в RAW таблице не найдено ни одной записи:",
                            "tblRawData пустая.")
    return rows


def get_origin_id(db: dbTolls, origin_name: str) -> int | None:
    """ Получает Id элемента справочника происхождения с именем origin_name. """
    if origin_name is None:
        return None
    origin_id = db.get_row_id(sql_origins["select_origin_name"], (origin_name,))
    if origin_id:
        return origin_id
    output_message_exit("в справочнике происхождения tblOrigins:",
                        f"не найдено записи с названием: {origin_name}.")


def get_origin_row_by_id(db: dbTolls, origin_id: int) -> sqlite3.Row | None:
    """ Получает запись элемента происхождения (учета данных)  с указанным id. """
    if origin_id is None or origin_id == 0:
        return None
    origin_row = db.go_select(sql_origins["select_origin_id"], (origin_id,))
    if origin_row:
        return origin_row[0]
    output_message_exit("в справочнике происхождения tblOrigins:",
                        f"не найдено записи с id: {origin_id}.")

def _get_supplement_number(db: dbTolls, period_id: int) -> int | None:
    """ Получает номер дополнения периода по id. """
    product_data = get_period_by_id(db, period_id)
    if product_data:
        return product_data['supplement_num']
    return None


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
    origin_id = get_origin_id(db, origin_name=catalog_name)
    item_id = get_directory_id(db, directory_team=directory_name, item_name=item_name)

    inserted_success, updated_success = [], []
    for db_row in raw_items:
        data = get_line_data(db, origin_id, db_row, item_id, period_id)
        # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
        # FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods
        # code, description, measurer, full_code
        raw_code, raw_period_id = data[4], data[3]
        # запоминаем номер дополнения для raw записи
        raw_period_supplement_num = _get_supplement_number(db, raw_period_id)
        # ищем продукт с таким же кодом в таблице tblProducts
        product = get_product_by_code(db=db, origin_id=origin_id, product_code=raw_code)
        if product:
            product_period_supplement_num = _get_supplement_number(
            db, product['FK_tblProducts_tblPeriods'])
            if raw_period_supplement_num >= product_period_supplement_num and item_id == product['FK_tblProducts_tblItems']:
                extra_data = data + (product['ID_tblProduct'],)
                count_updated = update_product(db, extra_data)
                if count_updated:
                    updated_success.append((id, raw_code))
            else:
                output_message_exit(
                    f"Ошибка обновления Продукта: {raw_code!r} или item_type не совпадает {item_id=!r}",
                    f"период Продукта {product['period']} старше загружаемого")
        else:
            inserted_id = insert_product(db, data)
            if inserted_id:
                inserted_success.append((id, raw_code))

    added = len(inserted_success)
    updated = len(updated_success)
    bug = len(raw_items) - (added + updated)
    message = f"{item_name:>12} : добавлено: {added:>5}, обновлено: {updated:>5}, ошибки: {bug:>5}"
    ic(message)
    # удалить item записи период которых меньше чем максимальный период
    delete_last_period_product_row(db, origin_id=origin_id, item_id=item_id)



def delete_raw_table(db_file: str):
    """ Удаляет временную таблицу tblRawData. """
    with (dbTolls(db_file) as db):
        db.go_execute(sql_raw_queries["delete_table_raw_data"])
        ic("Удалили tblRawData.")


def create_index_resources_raw_data(db_file_name: str):
    """Создать индекс tblRawData когда туда прочитаны Ресурсы (1, 2, 13)."""
    with dbTolls(db_file_name) as db:
        db.go_execute(sql_raw_queries["create_index_raw_data"])

def get_period_range(db_file: str, minimal_index_number: int) -> tuple[int, ...] | None:
    """
    Из таблицы периодов tblPeriods SQLite получает диапазон периодов id.Normative у которых
    номер индекса > minimal_index_number. id.Normative хранится в таблице tblPeriods.
    Возвращает отсортированный список кортежей (id.Normative, номер индексного периода).
    Сортировка по возрастанию номера индексного периода.
    period_range = [ (150862302, 198), (150996873, 199), ...]
    """
    if not db_file or not minimal_index_number:
        return None
    try:
        with dbTolls(db_file) as sqlite_db:
            result = sqlite_db.go_select(
                sql_periods_queries["get_periods_normative_id_index_num_more"],
                (minimal_index_number,),
            )
            if result:
                index_periods = [
                    (x["basic_database_id"], x["index_num"], x["supplement_num"])
                    for x in result
                ]
                index_periods.sort(reverse=False, key=lambda x: x[1])
                return index_periods
    except (IOError, sqlite3.OperationalError) as e:
        print(
            f"get_period_range: db_file={db_file}, minimal_index_number={minimal_index_number}"
        )
        print(f"Exception: {type(e).__name__}: {e}")
    except Exception as e:
        print("Unexpected error in get_period_range")
        print(f"db_file={db_file}, minimal_index_number={minimal_index_number}")
        print(f"Exception: {type(e).__name__}: {e}")
        raise
    return None

def get_indexes_for_supplement(db_file: str, supplement_number: int
) -> tuple[int, ...] | None:
    """
    Из таблицы периодов tblPeriods SQLite получает диапазон периодов id.Normative у которых
    """
    if not db_file or not supplement_number:
        return None
    try:
        with dbTolls(db_file) as db:
            result = db.go_select(
                sql_periods_queries["get_index_periods_for_supplement_tsn"],
                (supplement_number,),
            )
            if result:
                index_periods = [
                    (
                        x["basic_database_id"],
                        x["index_num"],
                        x["supplement_num"],
                        x["ID_tblPeriod"],
                    )
                    for x in result
                ]
                index_periods.sort(reverse=False, key=lambda x: x[1])
                return index_periods
    except (IOError, sqlite3.OperationalError) as e:
        print(
            f"get_indexes_for_supplement: db_file={db_file}, {supplement_number=}"
        )
        print(f"Exception: {type(e).__name__}: {e}")
    except Exception as e:
        print("Неожиданная ошибка в get_indexes_for_supplement")
        print(f"{db_file=}, {supplement_number=}")
        print(f"Exception: {type(e).__name__}: {e}")
        raise
    return None

def _insert_default_value_record_to_product(
    db: dbTolls, origin_id: int, period: int, default_code: str
) -> int | None:
    """
    Вставляет в tblProduct запись с шифром '0.0-0-0', для значений по умолчанию.
    Родителем корневая запись справочника.
    """
    target_item = ("default", "default")
    item_id = db.get_row_id(sql_items_queries["select_items_id_team_name"], target_item)
    if item_id is None:
        log = f"в справочнике tblItems: не найдена запись {target_item!r}"
        ic(log)
        return None
    # получаем ссылку на корневую запись каталога
    catalog_id = get_catalog_id_by_origin_code(db, origin_id, code=MAIN_RECORD_CODE)
    description = "Значение по умолчанию"
    data = (
        catalog_id,
        item_id,
        origin_id,
        period,
        default_code,
        description,
        None,
        code_to_number(default_code),
    )
    message = f"Вставка записи {default_code} {description!r} в Продукты"
    inserted_id = db.go_insert(sql_products_queries["insert_product"], data, message)
    if inserted_id:
        log = f"в продукты добавлен {default_code!r} {origin_id=}: {description!r} id: {inserted_id}"
        ic(log)
        return inserted_id
    output_message_exit(
        "В Продукты", f"Не добавлена запись {description} {default_code}"
    )


def update_product_default_value_record(
    db_file: str, catalog: str, period_id: int
) -> int | None:
    """
    Обновляет период у запись tblProducts 'Значение по умолчанию'.
    """
    with dbTolls(db_file) as db:
        default_code = clear_code(DEFAULT_RECORD_CODE)
        origin_id = get_origin_id(db, origin_name=catalog)
        default_record = get_product_by_code(db, origin_id, default_code)

        if default_code and origin_id and default_record:
            db.go_execute(
                sql_products_queries["update_product_period_by_id"],
                (period_id, default_record["ID_tblProduct"]),
            )
            count = db.go_execute(sql_products_queries["select_changes"])
            return count.fetchone()["changes"] if count else None
        else:
            inserted_id = _insert_default_value_record_to_product(
                db, origin_id=origin_id, period=period_id, default_code=default_code
            )
            return inserted_id
