import sqlite3
from icecream import ic
import numpy as np
from openpyxl.utils import get_column_letter
from typing import Optional

from config import dbTolls, LocalData, ROUNDING
from reports.sql_history_monitoring import sql_monitoring_history
from collections import namedtuple
from dataclasses import dataclass, field

from reports.report_excel_config import ExcelReport
from files_features import output_message_exit, output_message


Period = namedtuple(
    typename="Period",
    field_names=[
        "period_id",
        "supplement_number",
        "index_number",
        "title",
        ]
)
Period.__annotations__ = {
    "period_id": int,
    "supplement_number": int,
    "index_number": int,
    "title": str
}


Price = namedtuple(
    typename="Price",
    field_names=["supplier_price", "delivery", "index_num", "supplement_num"],
)
Price.__annotations__ = {
    "supplier_price": float,
    "delivery": bool,
    "index_num": int,
    "supplement_num": int,
}



# Material = namedtuple(
#     typename="Material",
#     field_names=["product_id", "code", "title", "monitoring_id", "digit_code", "prices"],
# )
# Material.__annotations__ = {
#     "product_id": int,
#     "code": str,
#     "title": str,
#     "monitoring_id": int,
#     "digit_code": int,
#     "prices": dict[int: Price] | None,
# }



@dataclass
class Material:
    product_id: int = 0
    code: str = ""
    title: str = ""
    monitoring_id: int = 0
    digit_code: int = 0
    prices: dict[int:Price] = (field(default_factory=dict),)




def _construct_period(row: sqlite3.Row) -> Optional[Period]:
    """Создает объект Period из строки запроса к базе данных"""
    if row is None:
        return None

    period_id = row["period_id"]
    supplement_number = row["supplement_number"]
    index_number = row["index_number"]
    title = row["title"]

    return Period(period_id=period_id, supplement_number=supplement_number,
                  index_number=index_number, title=title)

def _construct_material(row: sqlite3.Row) -> Optional[Material]:
    """Создает объект Material из строки запроса к базе данных"""
    if row is None:
        return None
    product_id=row["product_id"]
    code=row["code"]
    title=row["title"]
    monitoring_id=row["monitoring_id"]
    digit_code=row["digit_code"]
    return Material(
        product_id=product_id, code=code, title=title,
        monitoring_id=monitoring_id, digit_code=digit_code,
        prices=None
    )


def _get_unique_periods(db_file: str) -> list[Period] | None:
    """Получить уникальные периоды из истории мониторинга материалов"""
    with dbTolls(db_file) as db:
        result = db.go_select(sql_monitoring_history["select_unique_periods"])
        if result:
            periods = [_construct_period(row) for row in result]
            periods.sort(reverse=False, key=lambda x: x.index_number)
            return periods
    return None


def _get_unique_materials(db_file: str,) -> list[Material] | None:
    """Получить уникальные материалы из истории мониторинга материалов"""
    with dbTolls(db_file) as db:
        result = db.go_select(sql_monitoring_history["select_unique_materials"])
        if result:
            materials = [_construct_material(row) for row in result]
            materials.sort(reverse=False, key=lambda x: x.digit_code)
            return materials
    return None

def _construct_price(row: sqlite3.Row) -> Optional[Price]:
    """Создает объект Price из строки запроса к базе данных"""
    if row is None:
        return None
    return Price(
        supplier_price=row["last_supplier_price"],
        delivery=True if row["last_delivery"] == 1 else False,
        index_num=row["index_num"],
        supplement_num=row["supplement_num"],
    )


def _get_history_prices_for_material(db: dbTolls, monitoring_id: int) -> tuple:
    """Получить историю цен на материал по id."""
    prices = db.go_select(
        sql_monitoring_history["select_monitoring_history_prices_by_rowid"],
        ({"monitoring_id": monitoring_id}),
    )
    result = {price["index_num"]: _construct_price(price) for price in prices}
    return result

def _monitoring_price_history_report(
        periods: list[Period], materials: list[Material], sheet_name: str, file_name: str):
    """Напечатать отчет"""
    index_numbers = [period.index_number for period in periods]
    header = ["No", "Code", "Title", *index_numbers]
    with ExcelReport(file_name) as file:
        sheet = file.get_sheet(sheet_name)
        sheet.append(header)
        file.header_monitoring_price_format(sheet_name, row=1, header=header)

        for i, material in enumerate(materials, start=1):
            price_line = [
                    material.prices[x].supplier_price if material.prices.get(x, None) is not None else " "
                    for x in index_numbers
                ]
            price_row = [i, material.code, material.title, *price_line]
            sheet.append(price_row)



def _main_monitoring_materials_all_history_report():
    location = "office"  # office  # home
    local = LocalData(location)
    ic()

    periods = _get_unique_periods(local.db_file)
    ic(len(periods))
    ic(periods)
    materials = _get_unique_materials(local.db_file)
    ic(len(materials))
    # ic(materials[2:5])
    with dbTolls(local.db_file) as db:
        for material in materials:
            # ic(material)
            prices = _get_history_prices_for_material(db, material.monitoring_id)
            # ic(prices)
            material.prices = prices
            # ic(material)
    ic(materials[5])
    sheet_name = "monitoring_history_prices"
    file_name = "1_report_monitoring.xlsx"
    _monitoring_price_history_report(
        periods, materials, sheet_name=sheet_name, file_name=file_name
    )


if __name__ == "__main__":
    _main_monitoring_materials_all_history_report()


