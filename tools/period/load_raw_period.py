import sqlite3
import re
from pandas import DataFrame
from icecream import ic
from config import dbTolls
from excel_features import read_excel_to_df, read_csv_to_df
from tools import load_df_to_db_table
from config import LocalData, TON_ORIGIN
from tools.shared.shared_features import get_origin_id, get_directory_id, get_period_by_title
from tools.shared.code_tolls import text_cleaning, get_integer_value, date_parse
from sql_queries import sql_periods_queries
from files_features import output_message


def _load_xlsx_raw_data_periods(excel_file_name: str, sheet_name: str, db_full_file_name: str) -> int:
    """ Заполняет таблицу tblRawData данными из excel файла со страницы sheet_name данными выгрузки периодов. """
    df: DataFrame = read_excel_to_df(excel_file_name, sheet_name)
    # df.to_clipboard()
    return load_df_to_db_table(df, db_full_file_name, "tblRawData")


def _load_csv_raw_data_periods(csv_file_name: str, db_full_file_name: str) -> int:
    """ Заполняет таблицу tblRawData данными из csv файла  данными выгрузки периодов. """
    df: DataFrame = read_csv_to_df(csv_file_name)
    # df.to_clipboard()
    result = load_df_to_db_table(df, db_full_file_name, "tblRawData")
    return result

def _get_raw_data_by_pattern(db: dbTolls, column_name: str, pattern: str) -> list[sqlite3.Row] | None:
    """ Выбрать записи по столбцу column_name в соответствии с (re) паттерном из сырой таблицы. """
    query = f"SELECT * FROM tblRawData WHERE {column_name} REGEXP ?;"
    raw_lines = db.go_select(query, (pattern, ))
    if not raw_lines:
        output_message_exit(f"в RAW таблице с Периодами не найдено ни одной записи:",
                            f"tblRawData соответствующих шаблону {pattern!r} в поле {column_name!r}")
        return None
    return raw_lines

def _insert_period(db: dbTolls, data) -> int | None:
    """ Получает кортеж с данными периода для вставки в таблицу tblPeriods. """
    message = f"INSERT tblPeriods: {data!r}"
    inserted_id = db.go_insert(sql_periods_queries["insert_period"], data, message)
    if inserted_id:
        return inserted_id
    output_message(f"период: {data}", f"не добавлен в tblPeriods")
    return None


def _make_data_from_raw_supplement_ton(db: dbTolls, origin_id: int, category_id: int, raw_period: sqlite3.Row) -> tuple | None:
    """ Готовит данные 'ТСН Период Дополнение' для вставки в таблицу tblPeriods.
        Получает строку с исходными данными из таблицы tblRawData. Возвращает кортеж с данными для вставки. """
    # ['Начало', 'Тип периода', 'Статус', 'Окончание', 'Наименование', 'Индексный период', 'Комментарий',
    # 'Предыдущий период', 'Родительский период']
    title = text_cleaning(raw_period['Наименование'])
    supplement_num = get_integer_value(title.split()[1])
    index_num = 0
    date_start = date_parse(text_cleaning(raw_period['Начало']))
    comment = text_cleaning(raw_period['Комментарий'])
    ID_parent = None
    # title, supplement_num, index_num, date_start, comment, ID_parent,
    # FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods
    data = (title, supplement_num, index_num, date_start, comment, ID_parent, origin_id, category_id)
    return data

def _make_data_from_index_ton(db: dbTolls, origin_id: int, category_id: int, raw_period: sqlite3.Row) -> tuple | None:
    """ Готовит данные 'ТСН Период Индекс' для вставки в таблицу tblPeriods.
        Получает строку с исходными данными из таблицы tblRawData. Возвращает кортеж с данными для вставки. """
    # ['Начало', 'Тип периода', 'Статус', 'Окончание', 'Наименование', 'Индексный период', 'Комментарий',
    # 'Предыдущий период', 'Родительский период']
    title = text_cleaning(raw_period['Наименование'])
    pattern = r"^\s*(\d+)\s+индекс\/дополнение\s+(\d+)\s+(\(.+\))\s*$"
    result = re.match(pattern,title)
    # ic(result)
    # ic(result.groups())

    # title = re.sub(r"(\s+\(.+\))\s*$", "", title)
    title = f"Индекс {result.groups()[0]}"

    supplement_num = get_integer_value(result.groups()[1])
    index_num = get_integer_value(result.groups()[0])
    date_start = date_parse(text_cleaning(raw_period['Начало']))

    comment = text_cleaning(re.sub('[\(\)]','', result.groups()[2]))
    ID_parent = None
    # title, supplement_num, index_num, date_start, comment, ID_parent,
    # FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods
    data = (title, supplement_num, index_num, date_start, comment, ID_parent, origin_id, category_id)
    return data



def _periods_supplement_parsing(db_file: str):
    """ Запись периодов категории 'Дополнение' из tblRawData в боевую таблицу периодов. Для ТСН."""
    with dbTolls(db_file) as db:
        supplements_ton = _get_raw_data_by_pattern(db, column_name="[Наименование]", pattern="^\s*Дополнение\s+\d+\s*$")
        ic(len(supplements_ton))
        if supplements_ton is None:
            return None
        ton_origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
        ic(ton_origin_id)
        category_id = get_directory_id(db, directory_team="periods_category", item_name="supplement")
        ic(category_id)
        inserted_success, insert_fail = [], []
        for supplement in supplements_ton:
            data = _make_data_from_raw_supplement_ton(db, ton_origin_id, category_id, supplement)
            # ic(data)
            period_id = _insert_period(db, data)
            if period_id:
                inserted_success.append((period_id, data))
            else:
                insert_fail.append(data)
    return 0

def _index_periods_parsing(db_file: str):
    """ Запись периодов категории 'Индекс' из tblRawData в боевую таблицу периодов. Для ТСН."""
    with dbTolls(db_file) as db:
        pattern="^\s*\d+\s+индекс/дополнение\s+\d+\s+\(.+\)\s*$"
        indexes_ton = _get_raw_data_by_pattern(db, column_name="[Наименование]", pattern=pattern)
        ic(len(indexes_ton))
        if indexes_ton is None:
            return None
        ton_origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
        category_id = get_directory_id(db, directory_team="periods_category", item_name="index")

        ic(ton_origin_id, category_id)
        inserted_success, insert_fail = [], []
        for index in indexes_ton:
            data = _make_data_from_index_ton(db, ton_origin_id, category_id, index)
            # ic(data)
            period_id = _insert_period(db, data)
            if period_id:
                inserted_success.append((period_id, data))
            else:
                insert_fail.append(data)
    return 0

def parsing_raw_data_periods(data_paths: LocalData):
    """ 0: Success"""
    # csv_periods_file = data_paths.src_periods_data
    db_file = data_paths.db_file
    # result = _load_csv_raw_data_periods(csv_periods_file, db_file)
    # message = f"Данные по периодам прочитаны в tblRawData из файла {csv_periods_file!r}: {result=}"
    # ic(message)
    # !!!!!!!!!!!!
    with dbTolls(db_file) as db:
        db.go_execute(sql_periods_queries["delete_all_data_periods"])

    _periods_supplement_parsing(db_file)
    _index_periods_parsing(db_file)


    # select * from tblRawData where [Наименование] REGEXP '^\s*\d+\s+индекс/дополнение\s+\d+\s+\(.+\)\s*$';            #
    return 0


if __name__ == '__main__':
    from data_path import set_data_location

    location = "home"
    di = set_data_location(location)
    # print(di)
    r = parsing_raw_data_periods(di)
    ic(r)
