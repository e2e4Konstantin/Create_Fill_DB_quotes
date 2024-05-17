import sqlite3
from icecream import ic


from config import dbTolls, LocalData
from reports.sql_materials_report import sql_materials_reports
from reports.reports_config import Material, PriceHistory

from files_features import output_message_exit, output_message


def _fetch_supplement_number(db: dbTolls, index_number: int)-> int:
    result = db.go_select(
        sql_materials_reports[
            "select_ton_supplement_for_index"
        ],
        ({"ton_index_number": index_number}),
    )
    if not result:
        output_message_exit(
            f"для индексного периода: {index_number=}",
            "не найден номер дополнения.",
        )
    return result[0]["supplement_number"]


def _fetch_materials_from_products_history(db: dbTolls, supplement_number: int) -> sqlite3.Row:
    result = db.go_select(
        sql_materials_reports["select_products_from_history"],
        ({"supplement": supplement_number}),
    )
    if not result:
        output_message_exit(
            f"в _tblHistoryProducts для периода дополнения: {supplement_number=}",
            "не найдено ни одной записи.",
        )
    return result


def _get_data_materials_monitoring_for_period(
    db_file: str, index_number: int, monitoring_index_number: int
) -> list[Material] | None:
    """Получить материалы для дополнения и свойства для номера дополнения и данные мониторинга для номера индекса."""
    materials = []
    with dbTolls(db_file) as db:
        supplement_number = _fetch_supplement_number(db, index_number)
        materials = _fetch_materials_from_products_history(
            db, supplement_number
        )
        for material in materials[:10]:
            product_id = material["ID_tblProduct"]
            ic(product_id, material["code"])
    return materials if materials else None


if __name__ == "__main__":
    location = "office"  # office  # home
    local = LocalData(location)
    ic()

    table = _get_data_materials_monitoring_for_period(
        local.db_file, index_number=210, monitoring_index_number=211
    )
    if table:
        ic(len(table))
        ic(table[5])
