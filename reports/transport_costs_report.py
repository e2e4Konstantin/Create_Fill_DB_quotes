import sqlite3
from icecream import ic


from config import dbTolls, LocalData
from reports.sql_reports_transport_costs import sql_transport_costs_reports
from collections import namedtuple
from dataclasses import dataclass, field

from reports.report_excel_config import ExcelReport


PriceHistory = namedtuple(
    typename="PriceHistory", field_names=["history_index", "history_price"]
)
PriceHistory.__annotations__ = {"history_index": int, "history_price": float}


@dataclass
class TransportCost:
    code: str
    base_price: float
    index_num: int
    actual_price: float
    monitoring_index: int
    monitoring_price: float
    history: list[PriceHistory] = field(default_factory=list)


def get_history_prices(db: dbTolls, transport_cost_id: int, history_depth: int) -> list[PriceHistory]:
    """Получить историю цен транспортного расхода по id."""
    history = db.go_select(
        sql_transport_costs_reports[
            "select_historical_prices_for_transport_cost_id_not_empty_actual_price"
        ],
        (transport_cost_id, history_depth),
    )
    result =  [
        PriceHistory(history_index=row["index_num"], history_price=row["actual_price"])
        for row in history
    ]
    result.sort(reverse=False, key=lambda x: x.history_index)
    return result



def transport_costs_constructor(
    db: dbTolls, row: sqlite3.Row, history_depth: int
) -> TransportCost:
    tc = TransportCost(
        code=row["code"],
        base_price=row["base_price"],
        index_num=row["index_num"],
        actual_price=row["actual_price"],
        monitoring_index=row["monitoring_index"],
        monitoring_price=row["monitoring_price"],
        history=get_history_prices(db, row["ID_tblTransportCost"], history_depth),
    )
    return tc

def get_transport_cost_with_monitoring(
    db_file: str, history_depth: int
) -> list[TransportCost] | None:
    """Стоимость транспорта с данными последнего загруженного мониторинга."""
    table = None
    with dbTolls(db_file) as db:
        # получить id записей транспортных расходов с максимальным индексом периода и данными мониторинга
        transport_costs = db.go_select(
            sql_transport_costs_reports["select_records_for_max_index_with_monitoring"]
        )
        table = [transport_costs_constructor(db, line, history_depth) for line in transport_costs]
    return table if table else None


def report_output(table):
    """Напечатать отчет"""
    file = ExcelReport("report.xlsx")
    file.open_file()
    sheet = file.sheet_names[0]
    #
    header = ["шифр", "базовая стоимость", "actual index", "actual price", "monitoring index", "monitoring price"]
    header_index = ['', 'предыдущий индекс','текущий индекс', 'рост/абс','рост %']

    history_header = [x.history_index for x in table[0].history]
    mod_header = [*header[:2], *history_header, *header[2:], *header_index]

    file.write_header(sheet, mod_header)
    #
    # row = 3
    # for line in table:
    #     history_value = [x.history_price for x in line.history]
    #     row_value  = [line.code, line.base_price, line.index_num, line.actual_price, line.monitoring_index, line.monitoring_price]
    #     formulas = ["", "=J3/B3", "=L3/B3", "=O3-N3", "=(O3*100)/N3-100"]

    #     mod_row = [*row_value[:2], *history_value, *row_value[2:], *formulas]
    #     file.write_row(sheet, mod_row, row)
    #     #
    #     # file.write_format(sheet, row)

    #     row += 1
    file.save_file()




if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    table = get_transport_cost_with_monitoring(local.db_file, history_depth=6)
    ic(table)
    report_output(table)


