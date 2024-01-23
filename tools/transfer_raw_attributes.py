import sqlite3
from icecream import ic
from config import dbTolls
from sql_queries import sql_attributes_queries, sql_raw_queries, sql_products_queries
from files_features import output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value
from tools.shared_features import get_raw_data, get_product_all_catalog_by_code



def _make_data_from_raw_attribute(db: dbTolls, raw_attribute: sqlite3.Row) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированным атрибутом.
        Выбирает нужные данные, находит в расценках запись владельца атрибута.
        Возвращает кортеж с данными для вставки в таблицу Атрибутов tblAttributes.
    """
    raw_code = clear_code(raw_attribute["PRESSMARK"])
    raw_period = get_integer_value(raw_attribute["PERIOD"])
    raw_name = text_cleaning(raw_attribute['ATTRIBUTE_TITLE']).capitalize()
    raw_value = text_cleaning(raw_attribute['VALUE'])
    holder_product = get_product_all_catalog_by_code(db=db, product_code=raw_code)
    if holder_product:
        product_period = holder_product['period']
        product_id = holder_product['ID_tblProduct']
        if raw_period == product_period:
            return product_id, raw_name, raw_value
        else:
            output_message_exit(
                f"Ошибка загрузки Атрибута для продукта с шифром: {raw_code!r}",
                f"период Атрибута {raw_period} не равен текущему периоду владельца {product_period} ")
    else:
        output_message_exit(f"для Атрибута {tuple(raw_attribute)} не найдена запись Владельца",
                            f"шифр {raw_code!r}")
    return None


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
    """ Ищет в таблице Атрибутов атрибут по id владельца и названию атрибута."""
    attributes_cursor = db.go_execute(
        sql_attributes_queries["select_attributes_product_id_name"], attribute_data
    )
    attribute_row = attributes_cursor.fetchone() if attributes_cursor else None
    if attribute_row:
        return attribute_row['ID_Attribute']
    return 0



def transfer_raw_data_to_attributes(db_filename: str):
    """ Записывает атрибуты из сырой таблицы в рабочую tblAttributes.
        В таблице tblProducts ищется расценка с шифром который указан для Атрибута. """
    with dbTolls(db_filename) as db:
        raw_attributes = get_raw_data(db)
        inserted_attributes, deleted_attributes = [], []
        for row in raw_attributes:
            # ic(tuple(row))
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






