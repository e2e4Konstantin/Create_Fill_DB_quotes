import sqlite3
from icecream import ic
from config import dbTolls, LocalData
from reports.sql_reports_transport_costs import sql_transport_costs_reports
from collections import namedtuple
from dataclasses import dataclass, field


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


def get_history_prices(db: dbTolls, transport_cost_id: int) -> list[PriceHistory]:
    """Получить историю цен транспортного расхода по id."""
    history = db.go_select(
        sql_transport_costs_reports[
            "select_historical_prices_for_transport_cost_id_not_empty_actual_price"
        ],
        (transport_cost_id,),
    )
    return [
        PriceHistory(history_index=row["index_num"], history_price=row["actual_price"])
        for row in history
    ]



def transport_costs_constructor(db: dbTolls, row: sqlite3.Row) -> TransportCost:
    tc = TransportCost(
        code=row["code"],
        base_price=row["base_price"],
        index_num=row["index_num"],
        actual_price=row["actual_price"],
        monitoring_index=row["monitoring_index"],
        monitoring_price=row["monitoring_price"],
        history=get_history_prices(db, row["ID_tblTransportCost"]),
    )
    return tc

def get_transport_cost_with_monitoring(db_file: str) -> list[TransportCost] | None:
    """Стоимость транспорта с данными последнего загруженного мониторинга."""
    table = None
    with dbTolls(db_file) as db:
        # получить id записей транспортных расходов с максимальным индексом периода и данными мониторинга
        transport_costs = db.go_select(
            sql_transport_costs_reports["select_records_for_max_index_with_monitoring"]
        )
        table = [transport_costs_constructor(db, line) for line in transport_costs[:5]]
    return table if table else None



if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    table = get_transport_cost_with_monitoring(local.db_file)
    ic(table)

