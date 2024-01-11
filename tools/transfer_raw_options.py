import sqlite3
from icecream import ic
from config import dbTolls
from files_features import output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value, get_float_value

from sql_queries import sql_raw_queries, sql_products_queries, sql_options_queries
from tools.shared_features import get_raw_data


def _make_data_from_raw_option(db: dbTolls, raw_option: sqlite3.Row) -> tuple:
    """ Получает строку из таблицы tblRawData с импортированным параметром.
        Выбирает нужные данные, по шифру находит в tblProducts запись владельца параметра.
        Возвращает кортеж с данными для вставки в таблицу Атрибутов tblOptions.
    """
    raw_code = clear_code(raw_option["PRESSMARK"])
    raw_period = get_integer_value(raw_option["PERIOD"])
    raw_name = text_cleaning(raw_option['PARAMETER_TITLE']).capitalize()
    raw_left_border = get_float_value(raw_option['LEFT_BORDER'])
    raw_right_border = get_float_value(raw_option['RIGHT_BORDER'])
    raw_measurer = text_cleaning(raw_option['UNIT_OF_MEASURE'])
    raw_step = text_cleaning(raw_option['STEP'])
    raw_type = get_integer_value(raw_option['PARAMETER_TYPE'])

    # Найти product с шифром raw_cod в таблице tblProducts
    product = db.go_select(sql_products_queries["select_products_code"], (raw_code,))[0]
    if product:
        product_period = product['period']
        product_id = product['ID_tblProduct']
        if raw_period == product_period:
            return product_id, raw_name,  raw_left_border, raw_right_border, raw_measurer, raw_step, raw_type
        else:
            output_message_exit(
                f"Ошибка загрузки Параметра для продукта с шифром: {raw_code!r}",
                f"период Атрибута {raw_period} не равен текущему периоду владельца {product_period} ")
    else:
        output_message_exit(f"для Атрибута {tuple(raw_option)} не найдена Запись владельца",
                            f"шифр {raw_code!r}")
    return ()


def _delete_option(db: dbTolls, id_option: int) -> int:
    """ Удаляет запись из таблицы Параметров по ID_Option == id_option."""
    del_cursor = db.go_execute(sql_options_queries["delete_option_id"], (id_option,))
    return del_cursor.rowcount if del_cursor else 0


def _insert_option(db: dbTolls, option_data: tuple) -> int:
    """ Вставляет Параметр в таблицу Параметров. """
    message = f"INSERT tblOptions id продукта: {option_data[0]} параметр: {option_data[1:]}"
    inserted_id = db.go_insert(sql_options_queries["insert_option"], option_data, message)
    if not inserted_id:
        output_message_exit(f"параметр: {tuple(option_data)}", f"не добавлен в tblOptions")
    return inserted_id


def _get_option_id(db: dbTolls, option_data: tuple[int, str]) -> int:
    """ Ищет в таблице Параметров параметр по id Продукта и названию параметра."""
    options_cursor = db.go_execute(sql_options_queries["select_option_id_name"], option_data)
    option_row = options_cursor.fetchone() if options_cursor else None
    if option_row:
        return option_row['ID_Option']
    return 0


def transfer_raw_table_to_options(db_filename: str):
    """ Записывает Параметры из сырой таблицы tblRawData в рабочую таблицу tblOptions.
        В таблице tblProducts ищется расценка/продукт с шифром который указан для Параметра.
    """
    with dbTolls(db_filename) as db:
        raw_options = get_raw_data(db)
        inserted_options = []
        deleted_options = []
        for row in raw_options:
            data = _make_data_from_raw_option(db, row)
            # ищем в таблице рабочей параметров совпадающий параметр
            same_id = _get_option_id(db, data[:2])
            if same_id > 0:
                _delete_option(db, same_id)
                deleted_options.append(data)
            _insert_option(db, data)
            inserted_options.append(data)
        row_count = len(raw_options)
        alog = f"Всего raw записей в таблице параметров: {row_count}."
        ilog = f"Добавлено {len(inserted_options)} параметров в рабочую таблицу."
        dlog = f"Удалено {len(deleted_options)} совпадающих параметров."
        none_log = f"Непонятных записей: {row_count - len(inserted_options)}."
        ic(alog, ilog, dlog, none_log)

        print(len(deleted_options))
        mda = []
        for x in deleted_options:
            p = db.go_select(sql_products_queries["select_products_id"], (x[0],))[0]
            mda.append((x[0], p['code'], *x[1:]))
        ic(mda)
        for i in mda:
            print(i)


if __name__ == '__main__':
    import os
    from tools.create_tables import _create_options_environment
    from tools import read_csv_to_raw_table

    data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "Normative.sqlite3")
    options_data = os.path.join(data_path, "Расценки_10_68_split_options.csv")

    period = 68

    ic(db_name)
    ic(options_data)

    # with dbTolls(db_name) as db:
    #     _create_options_environment(db)

    read_csv_to_raw_table(db_name, options_data, period)

    # заполнить Параметры данными из таблицы tblRawData
    transfer_raw_table_to_options(db_name)
