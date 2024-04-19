from icecream import ic
from config import dbTolls, LocalData
from reports.sql_reports_transport_costs import sql_transport_costs_reports
from itertools import groupby


def transport_costs_report(location: LocalData) -> int:
    """
    История по транспортным расходам.
    """

    with dbTolls(location.db_file) as db:
        tab = []
        result = db.go_select(sql_transport_costs_reports["select_id_records_for_max_index"])
        for x in result[:6]:
            # ic(tuple(x), x["base_price"])        # ic(x["ID_tblTransportCost"])
            history = db.go_select(
                sql_transport_costs_reports["select_historical_prices_for_transport_cost_id"],
                (x["ID_tblTransportCost"],),
            )
            # ic(history[0].keys())
            l = [
                (
                    x["code"],
                    x["base_price"],
                    # x["description"],
                    # x["ID_tblTransportCost"],
                    p["index_num"],
                    p["actual_price"],
                )
                for p in history[:5]
            ]
            # ic(l)
            tab.extend(l)

        tab.sort(key=lambda x: x[2])
        ic(tab)
        head = ['code', 'base_price']
        body = {}

    mod = [(k, tuple(g)) for k, g in groupby(tab, lambda x: x[2])]
    ic(mod)
    for x in mod:
        head.append(x[0])
        for p in x[1]:
            body[p[0]] = [0, []]
    ic(head)
    ic(body)
    for x in mod:
        for p in x[1]:
            body[p[0]][0] = p[1]
            body[p[0]][1].append(p[3])
    ic(head)
    ic(body)


    return 0


if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    transport_costs_report(local)


# result = db.go_select(
#             sql_transport_costs_reports["select_period_id_for_max_index"]
#         )
#         id_period = result[0]["period_id"] if result else None
#         ic(id_period)

#         if id_period:
#             result = db.go_select(
#                 sql_transport_costs_reports["select_id_records_for_period_id"],
#                 (id_period,)
#             )
#         for x in result:
#             ic(x["id_tc"])