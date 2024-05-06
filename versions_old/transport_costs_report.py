import sqlite3
from icecream import ic
from config import dbTolls, LocalData
from reports.sql_transport_costs_reports import sql_transport_costs_reports
from itertools import groupby
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any

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


# # TransportCost = namedtuple(
# #     typename="TransportCost",
# #     field_names=[
# #         "code",
# #         "base_price",
# #         "index_num",
# #         "actual_price",
# #         "monitoring_price",
# #         "monitoring_index",
# #         "history"
# #     ],
# # )
# # TransportCost.__annotations__ = {
# #     "code": str,
# #     "base_price": float,
# #     "index_num": int,
# #     "actual_price": float,
# #     "monitoring_price": float,
# #     "monitoring_index": int,
# #     "history": list[tuple[PriceHistory]],
# # }


# # def get_history_transport_cost(db_file: str) -> list[TransportCost] | None:
# #     """ """
# #     table = []
# #     with dbTolls(db_file) as db:
# #         # получить id записей транспортных расходов с максимальным индексом периода
# #         transport_costs = db.go_select(
# #             sql_transport_costs_reports["select_records_for_max_index"]
# #         )
# #         for line_transport_cost in transport_costs:
# #             history = db.go_select(
# #                 sql_transport_costs_reports[
# #                     "select_historical_prices_for_transport_cost_id"
# #                 ],
# #                 (line_transport_cost["ID_tblTransportCost"],),
# #             )
# #             # ic(history[0].keys())
# #             extended_line_history = [
# #                 TransportCost(
# #                     line_transport_cost["code"],
# #                     line_transport_cost["base_price"],
# #                     line_history["index_num"],
# #                     line_history["actual_price"]
# #                 )
# #                 for line_history in history
# #             ]
# #             # ic(extended_line_history)
# #             table.extend(extended_line_history)
# #         table.sort(key=lambda x: x.index_num)
# #     return table if table else None


# # def transport_costs_report(location: LocalData) -> int:
# #     """
# #     История по транспортным расходам.
# #     """
# #     history = get_history_transport_cost(location.db_file)
# #     # ic(tab)
# #     # группируем по индексам
# #     groups_by_indexes = [
# #         (index_num, tuple(group))
# #         for index_num, group in groupby(history, lambda x: x.index_num)
# #     ]
# #     head = ["code", "base_price"]
# #     head.extend([x[0] for x in groups_by_indexes])
# #     head.append('monitoring')
# #     frame = {line.code: (line.base_price, []) for line in groups_by_indexes[0][1]}
# #     # заполняем цены
# #     for index_group in groups_by_indexes:
# #         for transport_cost in index_group[1]:
# #             frame[transport_cost.code][1].append(transport_cost.actual_price)
# #     # ic(head)
# #     # ic(frame)
# #     return head, frame


if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    l = get_transport_cost_with_monitoring(local.db_file)
    ic(l)
    # head, body  = transport_costs_report(local)
    # ic(body)

