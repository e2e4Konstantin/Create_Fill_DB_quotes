import sqlite3
from icecream import ic
import numpy as np
from openpyxl.utils import get_column_letter
from typing import Optional

from config import dbTolls, LocalData, ROUNDING
from reports.sql_materials_report import sql_materials_reports
from collections import namedtuple
from dataclasses import dataclass, field

from reports.report_excel_config import ExcelReport
from files_features import output_message_exit, output_message


PriceHistory = namedtuple(
    typename="PriceHistory", field_names=["history_index", "history_price"]
)
PriceHistory.__annotations__ = {"history_index": int, "history_price": float}

def sum_of_differences(values):
    if values is None or len(values) <= 3:
        return 1
    mean = sum(values) / len(values)
    return sum(abs(value - mean) for value in values)


def abbe_criterion(signal):
    """Критерий Аббе."""
    if signal is None or len(signal) <= 3:
        return 0
    differences = np.diff(signal) ** 2
    squares = np.sum(differences)
    return np.sqrt(squares / (len(signal) - 1))



@dataclass
class Material:
    code: str
    name: str
    base_price: float
    index_num: int
    actual_price: float
    monitoring_index: int
    monitoring_price: float
    #
    history_freight_included: int = 0
    history_freight_not_included: int = 0
    history_check: bool = False
    #
    transport_flag: bool = False
    transport_code: str = ""
    transport_base_price: float = 0.0
    transport_numeric_ratio: float = 0.0
    transport_actual_price: float = 0.0
    gross_weight: float = 0.0
    history: list[PriceHistory] = field(default_factory=list)
    len_history: int = 0
    abbe_criterion: float = 0.0
    mean_history: float = 0.0
    std_history: float = 0.0

    def __post_init__(self):
        if self.history is None:
            raise ValueError("history is None")
        self.len_history = len(self.history)
        self.abbe_criterion = round(
            abbe_criterion([x.history_price for x in self.history]), 3
        )
        self.mean_history = np.mean([x.history_price for x in self.history])
        self.std_history = np.std([x.history_price for x in self.history])
        self.transport_numeric_ratio = round(
            self.transport_actual_price / self.transport_base_price, ROUNDING
        ) if self.transport_base_price != 0 else 0




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
        #
        history_freight_included=row["history_freight_included"],
        history_freight_not_included=row["history_freight_not_included"],
        history_check=row["history_check"],
        #
        transport_flag=bool(row["transport_flag"]),
        transport_code=row["transport_code"],
        transport_base_price=row["transport_base_price"],
        transport_actual_price=row["transport_actual_price"],
        gross_weight=row["gross_weight"],
        #
        history=_get_material_price_history(db, row["ID_tblMaterial"], history_depth),
    )
    return material


def _get_materials_with_monitoring( db_file: str, history_depth: int) -> list[Material] | None:
    """Стоимость материалов с данными последнего загруженного мониторинга."""
    table = None
    with dbTolls(db_file) as db:
        # получить id записей материалов с максимальным индексом периода и данными мониторинга
        materials = db.go_select(
            sql_materials_reports["select_records_for_max_index_with_monitoring"]
        )
        table = [_materials_constructor(db, material, history_depth) for material in materials]
    return table if table else None



def _header_create(table: list[Material], view_history_depth: int) -> str:
    header = [
        "No",
        "шифр",
        "название",
        "базовая стоимость",
        "текущий индекс",  # actual index
        "текущая цена",  # actual price
        "мониторинг индекс",  # monitoring index
        "мониторинг цена",  # monitoring price
        #
        "история транспорт включен",  # history freight included
        "история без транспорта",  # history freight not included
        "нужна проверка",  # history check
        #
        "транспорт включен",  # transport flag
        "шифр транспорта",  # transport code
        "транспорт базовая цена",  # transport base price
        "транспорт коэффициент",  # transport numeric ratio
        "транспорт текущая цена",  # transport actual price
        #
        "вес брутто",  # gross weight
    ]
    header_calculated = [
            ".",
            "стоимость перевозки",
            "цена для загрузки",
            "предыдущий индекс",
            "текущий индекс",
            "рост абс.",
            "рост %",
            ".",
            "критерий разниц пар",
            'абс.',
            'внимание',
            'процент рост/падение',
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
            #
            if max_history_len > view_history_depth:
                history_header = history_header[-view_history_depth:]
                max_history_len = len(history_header)
            break
    final_header = [*header[:4], *history_header, *header[4:], *header_calculated]
    return final_header, max_history_len


def _material_row_create(material: Material, row_number: int, max_history_len: int):
    pos = {
        "row_count": [1, 0],
        "code":[2, material.code],
        "name": [3, material.name],
        "base_price": [4, material.base_price],
        #
        "history range": [5, None],
    }
    # вставляем пустые значения если история короче
    history_value = [x.history_price for x in material.history]
    if len(history_value) < max_history_len:
        for _ in range(max_history_len - len(history_value)):
            history_value.insert(0, None)
    else:
        history_value = history_value[-max_history_len:]
    #
    pos["history range"][1] = history_value
    history_end_column = pos["history range"][0]+len(history_value)
    #
    pos["index_num"] = [history_end_column, material.index_num]
    pos["actual_price"] = [history_end_column + 1, material.actual_price]
    pos["monitoring_index"] = [history_end_column + 2, material.monitoring_index]
    pos["monitoring_price"] = [history_end_column + 3, material.monitoring_price]
    #
    pos["history_freight_included"] = [history_end_column + 4, material.history_freight_included]
    pos["history_freight_not_included"] = [history_end_column + 5, material.history_freight_not_included]
    pos["history_check"] = [history_end_column + 6, material.history_check]
    #
    pos["transport_flag"] = [history_end_column + 7, material.transport_flag]
    pos["transport_code"] = [history_end_column + 8, material.transport_code]
    pos["transport_base_price"] = [history_end_column + 9, material.transport_base_price]
    pos["transport_numeric_ratio"] = [history_end_column + 10, material.transport_numeric_ratio]
    pos["transport_actual_price"] = [history_end_column + 11, material.transport_actual_price]
    #
    pos["gross_weight"] = [history_end_column + 12, material.gross_weight]
    pos["empty"] = [history_end_column + 13, ""]
    # формулы
    pos["transport_price"] = [history_end_column + 14, -77]
    pos["result_price"] = [history_end_column + 15, -77]
    pos["previous_index"] = [history_end_column + 16, -77]
    pos["result_index"] = [history_end_column + 17, -77]
    pos["abbe_criterion"] = [history_end_column + 21, material.abbe_criterion]
    pos["absolute_price_change"] = [history_end_column + 22, -77]
    #
    history_start_column = pos["history range"][0]
    history = {f"history_{i}":[history_start_column+i, v] for i, v in enumerate(pos["history range"][1])}
    # ic(history)
    pos.pop("history range")
    pos.update(history)

    for key in pos.keys():
        pos[key].append(get_column_letter(pos[key][0]))
    # формируем значения строки
    data = {value[0]: value[1] for value in pos.values()}
    data_pos = [data[x] for x in sorted(list(data.keys()))]
    # ic(data_pos)
    # ic(pos["transport_base_price"][0])
    # формулы
    formulas = [
        f"={pos['transport_base_price'][2]}{row_number}*{pos['transport_numeric_ratio'][2]}{row_number}*{pos['gross_weight'][2]}{row_number}/1000",
        f"=ROUND(IF({pos['transport_flag'][2]}{row_number}, ({pos['monitoring_price'][2]}{row_number}-{pos['transport_price'][2]}{row_number}), {pos['monitoring_price'][2]}{row_number}),2)",
        f"={pos['actual_price'][2]}{row_number}/{pos['base_price'][2]}{row_number}",
        f"={pos['result_price'][2]}{row_number}/{pos['base_price'][2]}{row_number}",
        f"={pos['result_index'][2]}{row_number}-{pos['previous_index'][2]}{row_number}",
        f"=({pos['result_index'][2]}{row_number}/{pos['previous_index'][2]}{row_number})-1",
        "",
        material.abbe_criterion,
        f"=ROUND(ABS({pos['result_price'][2]}{row_number}-{pos['actual_price'][2]}{row_number}),3)",
        f'=IF({pos["absolute_price_change"][2]}{row_number}<=1.4*{pos["abbe_criterion"][2]}{row_number}, "", 1)',
        f"=({pos['result_price'][2]}{row_number}/{pos['actual_price'][2]}{row_number})-1",
    ]
    mod_row = [*data_pos[:-6], *formulas]
    # ic(mod_row)
    return mod_row

def _materials_monitoring_report_output(table):
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
        row = 2
        for i, material in enumerate(table):
            value_row = _material_row_create(material, row, max_history_len)
            value_row[0] = i + 1
            file.write_row(sheet_name, value_row, row)
            file.write_material_format(sheet_name, row, max_history_len)
            row += 1
        ic()

def _modern_materials_monitoring_report_output(
    table: list[Material], view_history_depth: int, sheet_name: str, file_name: str
) -> None:
    """Напечатать отчет по мониторингу материалов"""
    with ExcelReport(file_name) as file:
        sheet = file.get_sheet(sheet_name)
        # ic(sheet)
        #
        header, max_history_len = _header_create(table, view_history_depth)
        file.write_header(sheet.title, header)
        #
        row = 2
        for i, material in enumerate(table):
            value_row = _material_row_create(material, row, max_history_len)
            value_row[0] = i + 1
            file.write_row(sheet_name, value_row, row)
            file.write_material_format(sheet_name, row, max_history_len)
            row += 1
        ic()


def _create_history_price_row(material: Material, max_history_len: int) -> list[Optional[float]]:
    """Создать строку с историей цен на материал"""
    first = [None] * (max_history_len - len(material.history))
    res = (
        first
        + [float(data.history_price) for data in material.history]
        + [
            round(material.abbe_criterion, 3)
            if material.abbe_criterion is not None
            else None
        ]
        + [
            round(material.mean_history, 3)
            if material.mean_history is not None
            else None
        ]
        + [
            round(material.std_history, 3)
            if material.std_history is not None
            else None
        ]
    )
    return res


def _material_price_history_report_output(
    table, sheet_name: str, file_name: str
):
    """Напечатать отчет по ценам материалов"""
    max_history_len = max(material.len_history for material in table)
    for material in table:
        if material.len_history == max_history_len:
            history_header = [str(x.history_index) for x in material.history]
            break
    history_header = ["No", "code", *history_header, "abbe criterion", "mean", "std"]

    row = 2
    with ExcelReport(file_name) as file:
        sheet = file.get_sheet(sheet_name)
        if history_header:
            sheet.append(history_header)
        for i, material in enumerate(table):
            price_row = _create_history_price_row(material, max_history_len)
            price_row = [i + 1, material.code, *price_row]
            sheet.append(price_row)
            row += 1


def last_period_main():
    location = "office"  # office  # home
    local = LocalData(location)
    ic()

    table = _get_materials_with_monitoring(local.db_file, history_depth=10)
    ic(len(table))
    ic(table[0])
    # for material in table[:2]:
    #     ic(material)

    sheet_name = "materials"
    file_name = "report_monitoring.xlsx"
    _modern_materials_monitoring_report_output(
        table, view_history_depth=3, sheet_name =sheet_name, file_name = file_name
    )
    _material_price_history_report_output(
        table, sheet_name="price materials history", file_name=file_name
    )

if __name__ == "__main__":
    last_period_main()


