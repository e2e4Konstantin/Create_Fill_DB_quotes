import sqlite3
import re

from icecream import ic
from collections import namedtuple
from typing import Literal
from pathlib import Path

# --
from config import dbTolls, MONTHS
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table

from config import LocalData, TON_ORIGIN, EQUIPMENTS_ORIGIN, MONITORING_ORIGIN
from tools.shared.shared_features import (
    get_origin_id,
    get_directory_id
)
from tools.shared.code_tolls import text_cleaning, get_integer_value, date_parse
from sql_queries import sql_periods_queries, sql_raw_queries
from files_features import output_message, output_message_exit


PairSI = namedtuple(typename="PairSI", field_names=["supplement", "index"])
PairSI.__annotations__ = {"supplement": int, "index": str}

def _get_raw_data_by_pattern(
    db: dbTolls, column_name: str, pattern: str
) -> list[sqlite3.Row] | None:
    """Выбрать записи по столбцу column_name в соответствии с (re) паттерном из таблицы tblRawData."""
    query = f"SELECT * FROM tblRawData WHERE {column_name} REGEXP ?;"
    raw_lines = db.go_select(query, (pattern,))
    if not raw_lines:
        output_message_exit(
            "в RAW таблице с Периодами не найдено ни одной записи:",
            f"tblRawData соответствующих шаблону {pattern!r} в поле {column_name!r}",
        )
        return None
    return raw_lines


def _insert_period(db: dbTolls, data) -> int | None:
    """Получает кортеж с данными периода для вставки в таблицу tblPeriods."""
    message = f"INSERT tblPeriods: {data!r}"
    inserted_id = db.go_insert(sql_periods_queries["insert_period"], data, message)
    if inserted_id:
        return inserted_id
    output_message(f"период: {data}", "не добавлен в tblPeriods")
    return None


def _get_supplement_index_numbers(
    kind: Literal[
        "equipment_supplement",
        "equipment_index",
        "ton_supplement",
        "ton_index",
        "monitoring_index",
    ],
    text: str,
) -> PairSI | None:
    match kind:
        case "equipment_supplement":
            # Оборудование (Глава 13-2)/дополнение 37
            pattern = r"^\s*[О|о]борудование\s+.*\/[Д|д]ополнение\s*(\d+)"
            result = re.match(pattern, text)
            if result:
                supplement_number = get_integer_value(result.groups()[0])
                return PairSI(supplement_number, 0)
            return None
        case "equipment_index":
            # 43 индекс/оборудование доп. 34 (мониторинг Февраль 2023)
            pattern = r"^\s*(\d+)\s+.*доп\.\s*(\d+)"
            result = re.match(pattern, text)
            if result:
                index_number = get_integer_value(result.groups()[0])
                supplement_number = get_integer_value(result.groups()[1])
                return PairSI(supplement_number, index_number)
            return None
        case "ton_supplement":
            # Дополнение 69
            pattern = r"^\s*[Д|д]ополнение\s*(\d+)"
            result = re.match(pattern, text)
            if result:
                supplement_number = get_integer_value(result.groups()[0])
                return PairSI(supplement_number, 0)
            return None
        case "ton_index":
            # 209 индекс/дополнение 71 (мониторинг Февраль 2024)
            pattern = r"^\s*(\d+)\s+индекс\/дополнение\s+(\d+)"
            result = re.match(pattern, text)
            if result:
                index_number = get_integer_value(result.groups()[0])
                supplement_number = get_integer_value(result.groups()[1])
                return PairSI(supplement_number, index_number)
            return None
        case "monitoring_index":
            # Мониторинг Февраль 2024 (209 сборник/дополнение 71)
            pattern = r"\((\d+)\s+сборник\/дополнение\s+(\d+).*\)"
            result = re.search(pattern, text)
            if result:
                index_number = get_integer_value(result.groups()[0])
                supplement_number = get_integer_value(result.groups()[1])
                return PairSI(supplement_number, index_number)
            return None
        case _:
            return None


def _ton_supplement_periods_parsing(db_file: str):
    """Запись периодов ТСН категории 'Дополнение' из tblRawData в боевую таблицу периодов.
    Устанавливает ID родительской записи на ту, где номер дополнения меньше на 1."""
    with dbTolls(db_file) as db:
        # "^\s*Дополнение\s+\d+\s*$"
        pattern = "^\s*Дополнение\s+(\d+|\d+\.\d+)\s*$"
        ton_supplements = _get_raw_data_by_pattern(
            db, column_name="[title]", pattern=pattern
        )
        # message = f"Прочитано ТСН Дополнений: {len(ton_supplements)}"
        # ic(message)
        if ton_supplements is None:
            return None
        origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
        category_id = get_directory_id(db, "periods_category", "supplement")
        # ic(origin_id, category_id)
        for line in ton_supplements:
            title = text_cleaning(line["title"])
            supplement, index = _get_supplement_index_numbers("ton_supplement", title)
            title = f"Дополнение {supplement}"
            date = date_parse(text_cleaning(line["date_start"]))
            comment = text_cleaning(line["cmt"])
            ID_parent = None
            basic_id = line["id"]
            # title, supplement_num, index_num, date_start, comment, ID_parent,
            # FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
            data = (
                title, supplement, index, date, comment,
                ID_parent, origin_id, category_id, basic_id,
            )
            _insert_period(db, data)
    return 0


def _ton_index_periods_parsing(db_file: str) -> int:
    """Запись периодов ТСН категории 'Индекс' из tblRawData в боевую таблицу периодов."""
    # 209 индекс/дополнение 71 (мониторинг Февраль 2024)
    with dbTolls(db_file) as db:
        pattern = "^\s*\d+\s+индекс\/дополнение\s+\d+\s+\(.+\)\s*$"
        ton_indexes = _get_raw_data_by_pattern(
            db, column_name="[title]", pattern=pattern
        )
        # message = f"Прочитано ТСН Индексов: {len(ton_indexes)}"
        # ic(message)
        if ton_indexes is None:
            return None
        origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
        category_id = get_directory_id(db, "periods_category", "index")
        # ic(origin_id, category_id)
        for line in ton_indexes:
            title = text_cleaning(line["title"])
            supplement, index = _get_supplement_index_numbers("ton_index", title)
            # в комментарий записываем то что в скобках
            cmt_pattern = "^.*\((.*)\)"
            result = re.match(cmt_pattern, title)
            comment = result.groups()[0] if result else None

            date = date_parse(text_cleaning(line["date_start"]))
            title = f"Индекс {index}"
            ID_parent = None
            basic_id = line["id"]
            # title, supplement_num, index_num, date_start, comment, ID_parent,
            # FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
            data = (
                title, supplement, index, date, comment, ID_parent,
                origin_id, category_id, basic_id,
            )
            _insert_period(db, data)
    return 0


def _equipment_supplement_periods_parsing(db_file: str) -> int:
    """Запись периодов 'Оборудование' категории 'Дополнение' из tblRawData в боевую таблицу периодов."""
    with dbTolls(db_file) as db:
        # Оборудование (Глава 13-2)/дополнение 37
        pattern = "^\s*[О|о]борудование\s+.*\/[Д|д]ополнение\s*(\d+)"
        supplements_equipment = _get_raw_data_by_pattern(
            db, column_name="[title]", pattern=pattern
        )
        # message = f"Прочитано Оборудование Дополнений: {len(supplements_equipment)}"
        # ic(message)
        if supplements_equipment is None:
            return None
        origin_id = get_origin_id(db, origin_name=EQUIPMENTS_ORIGIN)
        category_id = get_directory_id(
            db, directory_team="periods_category", item_name="supplement"
        )
        # ic(origin_id, category_id)
        for line in supplements_equipment:
            title = text_cleaning(line["title"])
            supplement_num, index_num = _get_supplement_index_numbers(
                "equipment_supplement", title
            )
            title = f"Оборудование - Дополнение {supplement_num}"
            date_start = date_parse(text_cleaning(line["date_start"]))
            comment = text_cleaning(line["cmt"])
            ID_parent = None
            basic_database_id = line["id"]
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
            _insert_period(db, data)
    return 0


def _equipment_index_periods_parsing(db_file: str) -> int:
    """Запись периодов 'Оборудование' категории 'Индекс' из tblRawData в боевую таблицу периодов."""
    with dbTolls(db_file) as db:
        # 49 индекс/оборудование доп. 36 (мониторинг Август 2023)
        pattern = "^\s*(\d+)\s*[И|и]ндекс\s*\/[О|о]борудование\s*доп\.\s*(\d+)"
        indexes = _get_raw_data_by_pattern(db, column_name="[title]", pattern=pattern)
        # message = f"Прочитано Оборудование Индексов: {len(indexes)}"
        # ic(message)
        if indexes is None:
            return None
        origin_id = get_origin_id(db, origin_name=EQUIPMENTS_ORIGIN)
        category_id = get_directory_id(
            db, directory_team="periods_category", item_name="index"
        )
        # ic(origin_id, category_id)
        for line in indexes:
            title = text_cleaning(line["title"])
            supplement, index = _get_supplement_index_numbers("equipment_index", title)
            title = f"Оборудование - Индекс {index } дополнение {supplement}"
            date = date_parse(text_cleaning(line["date_start"]))
            comment = text_cleaning(line["cmt"])
            ID_parent = None
            basic_id = line["id"]
            _insert_period(
                db,
                (
                    title,
                    supplement,
                    index,
                    date,
                    comment,
                    ID_parent,
                    origin_id,
                    category_id,
                    basic_id,
                ),
            )
    return 0


def _update_periods_parent(db_file: str, origin_name: str) -> int:
    """Обновляет данные периодов:
    получает id справочников,
    для Дополнений - родительское дополнение,
    для Индексов - родительский индекс,
    индекс для Дополнений - предыдущий (последний) индексный период.
    """
    with dbTolls(db_file) as db:
        # получить id типа справочника ТСН/Оборудование/Мониторинг
        origin_id = get_origin_id(db, origin_name=origin_name)
        dir_name = "periods_category"
        # получить id справочника "periods_category" для дополнений и индексов
        supplement_id = get_directory_id(db, dir_name, "supplement")
        index_id = get_directory_id(db, dir_name, "index")

        # обновить родителей для дополнений
        query = sql_periods_queries["update_periods_supplement_parent"]
        db.go_execute(query, {"id_origin": origin_id, "id_item": supplement_id})

        # обновить родителей для индексов
        query = sql_periods_queries["update_periods_index_parent"]
        db.go_execute(query, {"id_origin": origin_id, "id_item": index_id})

        # обновить номер индекса для дополнений - максимальный индекс для прошлого дополнения
        query = sql_periods_queries["update_periods_index_num_by_max"]
        db.go_execute(
            query,
            {
                "id_origin": origin_id,
                "id_item_index": index_id,
                "id_item_supplement": supplement_id,
            },
        )
    return 0





def _monitoring_index_periods_parsing(db_file: str) -> int | None:
    """Запись периодов 'Мониторинга' категории Индекс' из tblRawData в таблицу tblPeriods."""
    with dbTolls(db_file) as db:
        # Мониторинг Декабрь 2023 (207 сборник/дополнение 70)
        date_pattern = re.compile(
            "^.+(Январь|Февраль|Март|Апрель|Май|Июнь|Июль|Август|Сентябрь|Октябрь|Ноябрь|Декабрь)\s+(\d+)"
        )

        pattern = "^\s*Мониторинг"
        monitoring = _get_raw_data_by_pattern(
            db, column_name="[title]", pattern=pattern
        )
        if monitoring is None:
            return None
        origin_id = get_origin_id(db, origin_name=MONITORING_ORIGIN)
        category_id = get_directory_id(
            db, directory_team="periods_category", item_name="index"
        )
        for line in monitoring:
            title = text_cleaning(line["title"])

            result = _get_supplement_index_numbers("monitoring_index", title)
            if result:
                supplement_num, index_num = result
            else:
                output_message_exit("не смогли определить период мониторинга", f"{title}")

            result = date_pattern.match(title)
            if result:
                month = result.groups()[0]
                year = get_integer_value(result.groups()[1])
            else:
                output_message_exit("не смогли определить период мониторинга", f"{title}")
            monitor_number = (year % 100) * 1000 + MONTHS.get(month.lower(), 0)
            title = f"Мониторинг {month} {year} (индекс {index_num}/дополнение {supplement_num}) - {monitor_number}"

            date_start = date_parse(text_cleaning(line["date_start"]))
            comment = text_cleaning(line["cmt"])
            ID_parent = None
            basic_database_id = line["id"]
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
            _insert_period(db, data)
    return 0


# ----------------------------------------------------------------------------------------
def parsing_raw_periods(location: LocalData):
    """
    Читает данные о периодах из файла выгруженного из Postgres Normative
    larix.period в SQLite tblRawDat.  Удаляет все данные из таблицы
    периодов tblPeriods. Загружает периоды типа "Дополнение" и "Индекс"
    для раздела 'ТСН' и 'Оборудование'.
    """
    csv_periods_file = location.periods_file
    db_file = location.db_file
    load_csv_to_raw_table(csv_periods_file, db_file, delimiter=",")
    # message = f"Данные по периодам прочитаны в tblRawData из файла {csv_periods_file!r}"
    # ic(message)
    # Удалить все периоды !!!!!!!!!!!! и в tblRawData периоды старше 2020-01-01
    with dbTolls(db_file) as db:
        db.go_execute(sql_periods_queries["delete_all_data_periods"])
        db.go_execute(sql_raw_queries["delete_raw_data_old_periods"])

    # # переносим ТСН периоды
    _ton_supplement_periods_parsing(db_file)
    _ton_index_periods_parsing(db_file)
    _update_periods_parent(db_file, TON_ORIGIN)

    # переносим периоды для Оборудования
    _equipment_supplement_periods_parsing(db_file)
    _equipment_index_periods_parsing(db_file)
    _update_periods_parent(db_file, EQUIPMENTS_ORIGIN)

    # переносим периоды Мониторинга
    _monitoring_index_periods_parsing(db_file)
    _update_periods_parent(db_file, MONITORING_ORIGIN)
    #
    message = f"Периоды загружены из файла {Path(csv_periods_file).name!r}"
    ic(message)

    return 0


if __name__ == "__main__":
    parsing_raw_periods(LocalData("office"))
