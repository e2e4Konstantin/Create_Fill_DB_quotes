import sqlite3
from icecream import ic
from config import dbTolls
from sql_queries import sql_attributes_queries, sql_raw_queries, sql_products_queries
from files_features import output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value


def _make_data_from_raw_attribute(db: dbTolls, raw_attribute: sqlite3.Row) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированным атрибутом.
        Выбирает нужные данные, находит в расценках запись владельца атрибута.
        Возвращает кортеж с данными для вставки в таблицу Атрибутов tblAttributes.
    """
    raw_code = clear_code(raw_attribute["PRESSMARK"])
    raw_period = get_integer_value(raw_attribute["PERIOD"])
    raw_name = text_cleaning(raw_attribute['ATTRIBUTE_TITLE']).capitalize()
    raw_value = text_cleaning(raw_attribute['VALUE'])

    # Найти product с шифром raw_cod в таблице Расценок
    product = db.go_select(sql_products_queries["select_products_code"], (raw_code,))[0]
    if product:
        product_period = product['period']
        product_id = product['ID_tblProduct']
        if raw_period == product_period:
            return product_id, raw_name, raw_value
        else:
            output_message_exit(
                f"Ошибка загрузки Атрибута для расценки с шифром: {raw_code!r}",
                f"период Атрибута {raw_period} не равен текущему периоду расценки {product_period} ")
    else:
        output_message_exit(f"для Атрибута {tuple(raw_attribute)} не найдена Расценка",
                            f"шифр {raw_code!r}")
    return tuple()


def _delete_attribute(db: dbTolls, id_attribute: int) -> int:
    """ Удаляет запись из таблицы атрибутов с ID_Attribute = id_attribute."""
    del_cursor = db.go_execute(sql_attributes_queries["delete_attributes_id"], (id_attribute,))
    # mess = f"Из таблицы Атрибутов удалена запись с id = {id_attribute}"
    # ic(mess)
    return del_cursor.rowcount if del_cursor else 0


def _insert_attribute(db: dbTolls, data: tuple) -> int:
    """ Вставляет атрибут в таблицу Атрибутов """
    message = f"INSERT tblAttributes id расценки: {data[0]} атрибут: {data[1]!r} {data[2]!r}"
    inserted_id = db.go_insert(sql_attributes_queries["insert_attribute"], data, message)
    if not inserted_id:
        output_message_exit(f"атрибут {tuple(data)}", f"не добавлен в tblAttributes")
    return inserted_id


def _get_attribute_id(db: dbTolls, attribute_data: tuple[int, str]) -> int:
    """ Ищет в таблице Атрибутов атрибут по id Расценки и названию атрибута."""
    attributes_cursor = db.go_execute(sql_attributes_queries["select_attributes_id_name"], attribute_data)
    attribute_row = attributes_cursor.fetchone() if attributes_cursor else None
    if attribute_row:
        return attribute_row['ID_Attribute']
    return 0


def _get_raw_data_attributes(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все Атрибуты из сырой таблицы. """
    raw_attributes = db.go_select(sql_raw_queries["select_rwd_all"])
    if not raw_attributes:
        output_message_exit(f"в RAW таблице с Атрибутами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_attributes


def transfer_raw_table_to_attributes(db_filename: str):
    """ Записывает атрибуты из сырой таблицы в рабочую tblAttributes.
        Атрибуты которые надо добавить предварительно загружены в tblRawData.
        В таблице tblProducts ищется расценка с шифром который указан для Атрибута.
     """
    with dbTolls(db_filename) as db:
        raw_attributes = _get_raw_data_attributes(db)
        inserted_attributes = []
        deleted_attributes = []
        for row in raw_attributes:
            ic(tuple(row))
            data = _make_data_from_raw_attribute(db, row)
            # ищем в таблице атрибутов совпадающий атрибут
            same_id = _get_attribute_id(db, data[:2])
            if same_id > 0:
                _delete_attribute(db, same_id)
                deleted_attributes.append(data)
            _insert_attribute(db, data)
            inserted_attributes.append(data)


        row_count = len(raw_attributes)
        alog = f"Всего raw записей в таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_attributes)} атрибутов."
        dlog = f"Удалено {len(deleted_attributes)} совпадающих атрибутов."
        none_log = f"Непонятных записей: {row_count - len(inserted_attributes)}."
        ic(alog, ilog, dlog, none_log)

        # ic(deleted_attributes)
        print(len(deleted_attributes))
        mda = []
        for x in deleted_attributes:
            p = db.go_select(sql_products_queries["select_products_id"], (x[0], ))[0]
            mda.append((x[0], p['code'], *x[1:]))
        ic(mda)
        # for i in mda:
        #     print(i)






def _create_attributes_table(db_filename: str):
    """ Создать таблицы для хранения Атрибутов. """
    with dbTolls(db_filename) as db:
        db.go_execute(sql_attributes_queries["delete_table_attributes"])
        db.go_execute(sql_attributes_queries["delete_index_attributes"])
        db.go_execute(sql_attributes_queries["delete_view_attributes"])

        db.go_execute(sql_attributes_queries["create_table_attributes"])
        db.go_execute(sql_attributes_queries["create_index_attributes"])
        db.go_execute(sql_attributes_queries["create_view_attributes"])


if __name__ == '__main__':
    import os
    from tools import read_csv_to_raw_table

    # data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    data_path = r"C:\Users\kazak.ke\Documents\Задачи\Парсинг_параметризация\csv"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    period = 68

    db_name = os.path.join(db_path, "Normative.sqlite3")
    attributes_data = os.path.join(data_path, "Материалы_1_13_split_attributes.csv") # Расценки_4_68_split_attributes   /// Расценки_3_68_split_attributes // Материалы_1_13_split_attributes

    ic(db_name)
    ic(attributes_data)
    # _create_attributes_table(db_name)

    # прочитать из csv файла данные для Атрибутов в таблицу tblRawData для периода period
    read_csv_to_raw_table(db_name, attributes_data, period=68)

    # заполнить Атрибуты данными из таблицы tblRawData
    transfer_raw_table_to_attributes(db_name)
