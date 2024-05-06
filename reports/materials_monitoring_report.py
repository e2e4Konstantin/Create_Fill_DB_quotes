import sqlite3
from icecream import ic

from config import dbTolls, LocalData
from reports.sql_materials_report import sql_materials_reports
from collections import namedtuple
from dataclasses import dataclass, field

from reports.report_excel_config import ExcelReport


PriceHistory = namedtuple(
    typename="PriceHistory", field_names=["history_index", "history_price"]
)
PriceHistory.__annotations__ = {"history_index": int, "history_price": float}


@dataclass
class Material:
    code: str
    name: str
    base_price: float
    index_num: int
    actual_price: float
    monitoring_index: int
    monitoring_price: float
    history: list[PriceHistory] = field(default_factory=list)
    transport_flag: bool = False
    transport_code: str = ""
    transport_base_price: float = 0.0
    transport_factor: float = 0.0
    gross_weight: float = 0.0


def _get_material_price_history(
    db: dbTolls, transport_cost_id: int, history_depth: int
) -> list[PriceHistory]:
    """Получить историю цен на материал по id."""
    history = db.go_select(
        sql_materials_reports[
            "select_historical_prices_for_materials_id_not_empty_actual_price"
        ],
        (transport_cost_id, history_depth),
    )
    result = [
        PriceHistory(history_index=row["index_num"], history_price=row["actual_price"])
        for row in history
    ]
    result.sort(reverse=False, key=lambda x: x.history_index)
    return result


def _materials_constructor(
    db: dbTolls, row: sqlite3.Row, history_depth: int
) -> Material:
    material = Material(
        code=row["code"],
        name=row["title"],
        base_price=row["base_price"],
        index_num=row["index_num"],
        actual_price=row["actual_price"],
        monitoring_index=row["monitoring_index_num"],
        monitoring_price=row["monitoring_price"],
        history=_get_material_price_history(db, row["ID_tblMaterial"], history_depth),
        transport_flag=True if row["transport_flag"] else False,
        transport_code=row["transport_code"],
        transport_base_price=row["transport_base_price"],
        transport_factor=row["transport_factor"],
        gross_weight=row["gross_weight"],
    )
    return material


def get_materials_with_monitoring( db_file: str, history_depth: int) -> list[Material] | None:
    """Стоимость материалов с данными последнего загруженного мониторинга."""
    table = None
    with dbTolls(db_file) as db:
        # получить id записей материалов с максимальным индексом периода и данными мониторинга
        materials = db.go_select(
            sql_materials_reports["select_records_for_max_index_with_monitoring"]
        )
        table = [_materials_constructor(db, line, history_depth) for line in materials]
    return table if table else None


def materials_monitoring_report_output(table):
    """Напечатать отчет по мониторингу материалов"""
    sheet_name = "materials"
    file_name = "report_monitoring.xlsx"
    with ExcelReport(file_name) as file:
        sheet = file.get_sheet(sheet_name)
        ic(sheet)
        #
        header = [
            "No",
            "шифр",
            "название",
            "базовая стоимость",
            "actual index",
            "actual price",
            "monitoring index",
            "monitoring price",
            "transport_flag",
            "transport_code",
            "transport_base_price",
            "transport_factor",
            "gross_weight",
        ]
        header_index = ["", "стоимость перевозки", "предыдущий индекс", "текущий индекс", "рост/абс", "рост %"]

        history_header = [x.history_index for x in table[0].history]
        mod_header = [*header[:4], *history_header, *header[4:], *header_index]

        file.write_header(sheet.title, mod_header)

        row = 3
        for i, line in enumerate(table):
            row_value = [
                i+1,
                line.code,
                line.name,
                line.base_price,
                line.index_num,
                line.actual_price,
                line.monitoring_index,
                line.monitoring_price,
                line.transport_flag,
                line.transport_code,
                line.transport_base_price,
                line.transport_factor,
                line.gross_weight,
            ]
            history_value = [x.history_price for x in line.history]
            formulas = [
                "",
                f"=P{row}*Q{row}*R{row}/1000",
                f"=K{row}/D{row}",
                f"=M{row}/D{row}",
                f"=V{row}-U{row}",
                f"=(V{row}*100)/U{row}-100",
            ]
            mod_row = [*row_value[:4], *history_value, *row_value[4:], *formulas]
            file.write_row(sheet_name, mod_row, row)
            file.write_material_format(sheet_name, row, len(mod_row))

            row += 1
        ic()



if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    table = get_materials_with_monitoring(local.db_file, history_depth=5)
    ic(len(table), table[3])

    materials_monitoring_report_output(table)