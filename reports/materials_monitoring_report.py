import sqlite3
from icecream import ic
import numpy as np

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
    transport_flag: bool = False
    transport_code: str = ""
    transport_base_price: float = 0.0
    transport_factor: float = 0.0
    gross_weight: float = 0.0
    history: list[PriceHistory] = field(default_factory=list)
    len_history: int = 0

    def __post_init__(self):
        if self.history is None:
            raise ValueError("history is None")
        self.len_history = len(self.history)




def abbe_criterion(signal):
    """Критерий Аббе."""
    if signal is None or len(signal) <= 2:
        return 0
    differences = np.diff(signal) ** 2
    squares = np.sum(differences)
    return np.sqrt(squares / (len(signal)-1))


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
        transport_flag=bool(row["transport_flag"]),
        transport_code=row["transport_code"],
        transport_base_price=row["transport_base_price"],
        transport_factor=row["transport_factor"],
        gross_weight=row["gross_weight"],
        history=_get_material_price_history(db, row["ID_tblMaterial"], history_depth),
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

def _header_create(table: list[Material]) -> str:
    header = [
            "No",
            "шифр",
            "название",
            "базовая стоимость",
            "actual index",
            "actual price",
            "monitoring index",
            "monitoring price",
            "transport flag",
            "transport code",
            "transport base price",
            "transport factor",
            "gross weight",
        ]
    header_calculated = [
            "",
            "стоимость перевозки",
            "цена для загрузки",
            "предыдущий индекс",
            "текущий индекс",
            "рост абс.",
            "рост %",
            "",
            "abbe критерий",
        ]

    history_sizes = set([material.len_history for material in table])
    ic(history_sizes)  #  {1, 4, 5}
    max_history_len = max(history_sizes)
    history_header = []
    #  найти материал с длинной историей и записать заголовок
    for material in table:
        if material.len_history == max_history_len:
            # ic(material)
            history_header = [x.history_index for x in material.history]
            break
    final_header = [*header[:4], *history_header, *header[4:], *header_calculated]
    return final_header, max_history_len


def _material_row_create(material: Material, row_number: int, max_history_len: int):
    base_value = [
                0,
                material.code,
                material.name,
                material.base_price,
                material.index_num,
                material.actual_price,
                material.monitoring_index,
                material.monitoring_price,
                material.transport_flag,
                material.transport_code,
                material.transport_base_price,
                material.transport_factor,
                material.gross_weight,
            ]
    history_value = [x.history_price for x in material.history]
    abbe = abbe_criterion(history_value)
    if len(history_value) < max_history_len:
        for _ in range(max_history_len - len(history_value)):
            history_value.insert(0, None)

    formulas = [
        "",
        f"=P{row_number}*Q{row_number}*R{row_number}/1000",
        f"=ROUND(IF(N{row_number}, (M{row_number}-T{row_number}), M{row_number}),2)",
        f"=K{row_number}/D{row_number}",
        f"=M{row_number}/D{row_number}",
        f"=W{row_number}-V{row_number}",
        f"=(W{row_number}*100)/V{row_number}-100",
        "",
        round(abbe, 3),
        f"=ROUND(ABS(U{row_number}-K{row_number}),3)",
        f'=IF(AB{row_number}<=1.5*AA{row_number}, "", 1)',
        f"=(U{row_number}/K{row_number})-1",
    ]
    mod_row = [*base_value[:4], *history_value, *base_value[4:], *formulas]
    # mod_row.append(abbe)
    return mod_row

def materials_monitoring_report_output(table):
    """Напечатать отчет по мониторингу материалов"""
    sheet_name = "materials"
    file_name = "report_monitoring.xlsx"
    with ExcelReport(file_name) as file:
        sheet = file.get_sheet(sheet_name)
        ic(sheet)
        #
        header, max_history_len = _header_create(table)
        file.write_header(sheet.title, header)
        #
        row = 3
        for i, material in enumerate(table):
            value_row = _material_row_create(material, row, max_history_len)
            value_row[0] = i + 1
            file.write_row(sheet_name, value_row, row)
            file.write_material_format(sheet_name, row, len(value_row))
            row += 1
        ic()

check_list = (
    "1.1-1-1",
    "1.12-5-2296",
    "1.12-5-2297",
    "1.12-5-2298",
    "1.12-7-199",
    "1.12-11-759",
    '1.7-14-599',

)

if __name__ == "__main__":
    # import openpyxl

    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    table = get_materials_with_monitoring(local.db_file, history_depth=5)

    # for material in table:
    #     if material.len_history == 4:
    #         ic(material)

    ic(len(table), table[3])
    # check_table = []
    # for material in table:
    #     if material.code in check_list:
    #         ic(material)
    #         check_table.append(material)


    materials_monitoring_report_output(table)


