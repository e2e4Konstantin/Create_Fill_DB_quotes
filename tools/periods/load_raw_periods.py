import sqlite3
import re
from pandas import DataFrame
from icecream import ic
from config import dbTolls
from excel_features import read_excel_to_df, read_csv_to_df
from tools.shared.load_df_to_db_table import load_df_to_db_table
from config import LocalData, TON_ORIGIN
from tools.shared.shared_features import (
    get_origin_id,
    get_directory_id,
    get_period_by_title,
)
from tools.shared.code_tolls import text_cleaning, get_integer_value, date_parse
from sql_queries import sql_periods_queries
from files_features import output_message


def _load_xlsx_raw_data_periods(
    excel_file_name: str, sheet_name: str, db_full_file_name: str
) -> int:
    """Заполняет таблицу tblRawData данными из excel файла со страницы sheet_name данными выгрузки периодов."""
    df: DataFrame = read_excel_to_df(excel_file_name, sheet_name)
    # df.to_clipboard()
    return load_df_to_db_table(df, db_full_file_name, "tblRawData")


def _load_csv_raw_data_periods(
    csv_file_name: str, db_full_file_name: str, delimiter: str = ";"
) -> int:
    """Заполняет таблицу tblRawData данными из csv файла  данными выгрузки периодов."""
    df: DataFrame = read_csv_to_df(csv_file_name, delimiter)
    # df.to_clipboard()
    result = load_df_to_db_table(df, db_full_file_name, "tblRawData")
    return result


def _get_raw_data_by_pattern(
    db: dbTolls, column_name: str, pattern: str
) -> list[sqlite3.Row] | None:
    """Выбрать записи по столбцу column_name в соответствии с (re) паттерном из сырой таблицы."""
    query = f"SELECT * FROM tblRawData WHERE {column_name} REGEXP ?;"
    raw_lines = db.go_select(query, (pattern,))
    if not raw_lines:
        output_message_exit(
            f"в RAW таблице с Периодами не найдено ни одной записи:",
            f"tblRawData соответствующих шаблону {pattern!r} в поле {column_name!r}",
        )
        return None
    return raw_lines


def _insert_period(db: dbTolls, data) -> int | None:
    """Получает кортеж с данными периода для вставки в таблицу tblPeriods."""
    message = f"INSERT tblPeriods: {data!r}"
    inserted_id = db.go_insert(
        sql_periods_queries["insert_period"], data, message)
    if inserted_id:
        return inserted_id
    output_message(f"период: {data}", f"не добавлен в tblPeriods")
    return None


def _make_data_from_raw_supplement_ton(
    db: dbTolls, origin_id: int, category_id: int, raw_period: sqlite3.Row
) -> tuple | None:
    """Готовит данные 'ТСН Период Дополнение' для вставки в таблицу tblPeriods.
    Получает строку с исходными данными из таблицы tblRawData. Возвращает кортеж с данными для вставки.
    """
    # [id, date_start, period_type, date_end, title, is_infl_rate, cmt, created_on, created_by, modified_on,
    # modified_by, parent_id, previous_id, base_type_code, deleted_on]

    title = text_cleaning(raw_period["title"])
    raw_number = title.split()[1].split(".")[0]
    supplement_num = get_integer_value(raw_number)
    index_num = 0
    date_start = date_parse(text_cleaning(raw_period["date_start"]))
    comment = text_cleaning(raw_period["cmt"])
    ID_parent = None
    basic_database_id = raw_period["id"]
    # title, supplement_num, index_num, date_start, comment, ID_parent,
    # FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
    data = (
        title,
        supplement_num,
        index_num,
        date_start,
        comment,
        ID_parent,
        origin_id,
        category_id,
        basic_database_id,
    )
    return data


def _make_data_from_index_ton(
    db: dbTolls, origin_id: int, category_id: int, raw_period: sqlite3.Row
) -> tuple | None:
    """Готовит данные 'ТСН Период Индекс' для вставки в таблицу tblPeriods.
    Получает строку с исходными данными из таблицы tblRawData. Возвращает кортеж с данными для вставки.
    """
    # [id, date_start, period_type, date_end, title, is_infl_rate, cmt, created_on, created_by, modified_on,
    # modified_by, parent_id, previous_id, base_type_code, deleted_on]
    title = text_cleaning(raw_period["title"])
    # 209 индекс/дополнение 71 (мониторинг Февраль 2024)
    pattern = r"^\s*(\d+)\s+индекс\/дополнение\s+(\d+)\s+(\(.+\))\s*$"
    result = re.match(pattern, title)
    # ic(result)
    # ic(result.groups())

    # title = re.sub(r"(\s+\(.+\))\s*$", "", title)
    title = f"Индекс {result.groups()[0]}"

    supplement_num = get_integer_value(result.groups()[1])
    index_num = get_integer_value(result.groups()[0])
    date_start = date_parse(text_cleaning(raw_period["date_start"]))

    comment = text_cleaning(re.sub("[\(\)]", "", result.groups()[2]))
    ID_parent = None
    basic_database_id = raw_period["id"]
    # title, supplement_num, index_num, date_start, comment, ID_parent,
    # FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
    data = (
        title,
        supplement_num,
        index_num,
        date_start,
        comment,
        ID_parent,
        origin_id,
        category_id,
        basic_database_id,
    )
    return data


def _ton_supplement_periods_parsing(db_file: str):
    """Запись периодов ТСН категории 'Дополнение' из tblRawData в боевую таблицу периодов.
    Устанавливает ID родительской записи на ту, где номер дополнения меньше на 1."""
    with dbTolls(db_file) as db:
        # "^\s*Дополнение\s+\d+\s*$"
        pattern = "^\s*Дополнение\s+(\d+|\d+\.\d+)\s*$"
        supplements_ton = _get_raw_data_by_pattern(
            db, column_name="[title]", pattern=pattern
        )
        ic(len(supplements_ton))
        if supplements_ton is None:
            return None
        ton_origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
        category_id = get_directory_id(
            db, directory_team="periods_category", item_name="supplement"
        )
        ic(ton_origin_id, category_id)
        # inserted_success, insert_fail = [], []
        for supplement in supplements_ton:
            data = _make_data_from_raw_supplement_ton(
                db, ton_origin_id, category_id, supplement
            )
            # ic(data)
            period_id = _insert_period(db, data)
            # if period_id:
            #     inserted_success.append((period_id, data))
            # else:
            #     insert_fail.append(data)
        db.go_execute(
            sql_periods_queries["update_periods_supplement_parent"], (
                ton_origin_id, category_id)
        )
    return 0


def _ton_index_periods_parsing(db_file: str):
    """Запись периодов ТСН категории 'Индекс' из tblRawData в боевую таблицу периодов."""
    with dbTolls(db_file) as db:
        pattern = "^\s*\d+\s+индекс\/дополнение\s+\d+\s+\(.+\)\s*$"
        indexes_ton = _get_raw_data_by_pattern(
            db, column_name="[title]", pattern=pattern
        )
        ic(len(indexes_ton))
        if indexes_ton is None:
            return None
        ton_origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
        category_id = get_directory_id(
            db, directory_team="periods_category", item_name="index"
        )
        ic(ton_origin_id, category_id)

        for index in indexes_ton:
            data = _make_data_from_index_ton(
                db, ton_origin_id, category_id, index)
            period_id = _insert_period(db, data)
        db.go_execute(
            sql_periods_queries["update_periods_index_parent"], (
                ton_origin_id, category_id)
        )
    return 0


def parsing_raw_periods(data_paths: LocalData):
    """ Читает данные о периодах из выгруженного файла из основной базы.
        Удаляет все данные из таблицы периодов.
        Загружает периоды типа "Дополнение" и "Индексы" для раздела ТСН.
    0: Success"""
    csv_periods_file = data_paths.src_periods_data
    db_file = data_paths.db_file
    result = _load_csv_raw_data_periods(
        csv_periods_file, db_file, delimiter=",")
    message = f"Данные по периодам прочитаны в tblRawData из файла {csv_periods_file!r}: {result=}"
    ic(message)
    # Удалить все периоды !!!!!!!!!!!!
    with dbTolls(db_file) as db:
        db.go_execute(sql_periods_queries["delete_all_data_periods"])
    _ton_supplement_periods_parsing(db_file)
    _ton_index_periods_parsing(db_file)
    return 0


if __name__ == "__main__":
    from data_path import set_data_location

    location = "home"  # office
    di = set_data_location(location)
    r = parsing_raw_periods(di)