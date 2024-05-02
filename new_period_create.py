from config import LocalData
from icecream import ic

from sql_queries import sql_periods_queries
from config import dbTolls, MONITORING_ORIGIN, MONTHS
from files_features import output_message, output_message_exit
from tools.shared.shared_features import get_origin_id, get_directory_id



def _insert_period(db: dbTolls, data) -> int | None:
    """Получает кортеж с данными периода для вставки в таблицу tblPeriods."""
    message = f"INSERT tblPeriods: {data!r}"
    inserted_id = db.go_insert(sql_periods_queries["insert_period"], data, message)
    if inserted_id:
        return inserted_id
    output_message(f"период: {data}", "не добавлен в tblPeriods")
    return None


def _monitoring_index_period_prepare(db_file: str, supplement_num: int, index_num: int, year: int, month: str, line) -> int | None:
    """Запись периодов 'Мониторинга' категории Индекс' из tblRawData в таблицу tblPeriods."""
    with dbTolls(db_file) as db:

        origin_id = get_origin_id(db, origin_name=MONITORING_ORIGIN)
        category_id = get_directory_id(
            db, directory_team="periods_category", item_name="index"
        )


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


if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()