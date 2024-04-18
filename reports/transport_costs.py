from icecream import ic
from config import dbTolls, LocalData
from reports.sql_transport_costs import sql_transport_costs_reports


def transport_costs_report(location: LocalData) -> int:
    """
    История по транспортным расходам.
    """

    with dbTolls(location.db_file) as db:
        result = db.go_select(
            sql_transport_costs_reports["select_period_id_for_max_index"]
        )
        id_period = result[0]["ID_tblPeriod"] if result else None
        ic(id_period)

        if id_period:
            result = db.go_select(
                sql_transport_costs_reports["select_id_records_for_period_id"],
                (id_period,)
            )
        for x in result:
            ic(x["id_tc"])


    return 0


if __name__ == "__main__":
    location = "office"  # office  # home

    local = LocalData(location)
    ic()
    transport_costs_report(local)
