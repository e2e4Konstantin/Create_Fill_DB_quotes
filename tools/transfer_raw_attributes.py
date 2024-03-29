import sqlite3
from icecream import ic
from config import dbTolls, PNWC_ORIGIN, TON_ORIGIN
from sql_queries import sql_attributes_queries, sql_raw_queries, sql_products_queries
from files_features import output_message_exit
from tools.shared.code_tolls import clear_code, text_cleaning, get_integer_value
from tools.shared.shared_features import get_raw_data, get_origin_id


def _make_data_from_raw_attribute(db: dbTolls, raw_attribute: sqlite3.Row, catalogs_id: tuple[int, int]) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированным атрибутом.
        Выбирает нужные данные, находит в расценках запись владельца атрибута.
        Возвращает кортеж с данными для вставки в таблицу Атрибутов tblAttributes.
    """
    ton_catalog_id, pnwc_catalog_id = catalogs_id
    raw_code = clear_code(raw_attribute["PRESSMARK"])
    raw_period = get_integer_value(raw_attribute["PERIOD"])
    raw_name = text_cleaning(raw_attribute['ATTRIBUTE_TITLE']).capitalize()
    raw_value = text_cleaning(raw_attribute['VALUE'])

    # Найти product с шифром raw_cod в таблице tblProducts
    products = db.go_select(sql_products_queries["select_product_all_code"], (raw_code,))
    if products:
        product = products[0]
        product_period = product['period']
        catalog_id = product['FK_tblProducts_tblOrigins']
        if (catalog_id == ton_catalog_id and raw_period == product_period) or (catalog_id == pnwc_catalog_id):
            return product['ID_tblProduct'], raw_name, raw_value
        else:
            output_message_exit(
                f"Ошибка загрузки Атрибута для продукта с шифром: {raw_code!r}",
                f"период Атрибута {raw_period} не равен текущему периоду владельца {product_period} ")

    else:
        output_message_exit(f"для Атрибута {tuple(raw_attribute)} не найдена запись Владельца",
                            f"шифр {raw_code!r}")
    return ()


def _delete_attribute(db: dbTolls, id_attribute: int) -> int:
    """ Удаляет запись из таблицы атрибутов с ID_Attribute == id_attribute."""
    del_cursor = db.go_execute(sql_attributes_queries["delete_attributes_id"], (id_attribute,))
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
    attributes_cursor = db.go_execute(sql_attributes_queries["select_attributes_product_id_name"], attribute_data)
    attribute_row = attributes_cursor.fetchone() if attributes_cursor else None
    if attribute_row:
        return attribute_row['ID_Attribute']
    return 0


def transfer_raw_data_to_attributes(db_filename: str):
    """ Записывает атрибуты из сырой таблицы в рабочую tblAttributes.
        Атрибуты которые надо добавить предварительно загружены в tblRawData.
        В таблице tblProducts ищется расценка с шифром который указан для Атрибута.
    """
    with dbTolls(db_filename) as db:
        raw_attributes = get_raw_data(db)
        ton_catalog_id = get_origin_id(db, origin_name=TON_ORIGIN)
        pnwc_catalog_id = get_origin_id(db, origin_name=PNWC_ORIGIN)
        inserted_attributes = []
        deleted_attributes = []
        for row in raw_attributes:
            # ic(tuple(row))
            data = _make_data_from_raw_attribute(db, row, catalogs_id=(ton_catalog_id, pnwc_catalog_id))
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
            p = db.go_select(sql_products_queries["select_products_id"], (x[0],))[0]
            mda.append((x[0], p['code'], *x[1:]))
        ic(mda)
        # for i in mda:
        #     print(i)


if __name__ == '__main__':
    import os
    from tools.create_tables import _create_attributes_environment
    from tools import read_csv_to_raw_table

    data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    # data_path = r"C:\Users\kazak.ke\Documents\Задачи\Парсинг_параметризация\csv"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    period = 68

    db_name = os.path.join(db_path, "Normative.sqlite3")
    attributes_data = os.path.join(data_path,
                                   "Оборудование_13_68_split_attributes.csv")  # Расценки_4_68_split_attributes   /// Расценки_3_68_split_attributes // Материалы_1_13_split_attributes

    ic(db_name)
    ic(attributes_data)
    with dbTolls(db_name) as db:
        _create_attributes_environment(db)

    # прочитать из csv файла данные для Атрибутов в таблицу tblRawData для периода period
    read_csv_to_raw_table(db_name, attributes_data, period=68)

    # заполнить Атрибуты данными из таблицы tblRawData
    # transfer_raw_table_to_attributes(db_name)
