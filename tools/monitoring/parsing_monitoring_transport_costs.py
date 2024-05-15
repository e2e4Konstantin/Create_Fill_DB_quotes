import sqlite3
from icecream import ic

from config import dbTolls, LocalData, Period, MONITORING_ORIGIN, TON_ORIGIN, ROUNDING
from files_features import output_message_exit, create_abspath_file
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table

from sql_queries import (
    sql_raw_queries,
    sql_monitoring_transport_costs,
)

from tools.shared.shared_features import (
    get_product_by_code,
    get_origin_id,
    get_raw_data,
    get_period_by_origin_and_numbers,
    get_period_by_id,
)
from tools.shared.code_tolls import (
    clear_code,
    get_float_value,
    get_integer_value,
    text_cleaning,
)


def _round_raw_current_price(db_file: str, rounding_digits):
    """Округлить до rounding_digits знаков цену на транспортные услуги."""
    with dbTolls(db_file) as db:
        db.go_execute(
            sql_raw_queries["round_price_for_monitoring_transport_costs"],
            (rounding_digits,),
        )

def _update_monitoring_transport_costs(
    db: dbTolls, transport_cost: sqlite3.Row, data: tuple
) -> int:
    """Обновляет материал мониторинга данными из data."""
    transport_cost_id = transport_cost["ID_MonitoringTransportCost"]
    transport_cost_period = get_period_by_id(
        db, transport_cost["FK_tblMonitoringTransportCosts_tblPeriods"]
    )
    index_num = transport_cost_period["index_num"]
    raw_index = data[3].index
    if raw_index >= index_num:
        data = (*data[:-1], transport_cost_id)
        db.go_execute(
            sql_monitoring_transport_costs["update_monitoring_transport_costs_by_id"],
            data,
        )
        return 0
    else:
        output_message_exit(
            f"Ошибка обновления 'Мониторинга Транспортного расхода': {tuple(transport_cost)}",
            f"номер индекса {index_num} больше загружаемого {raw_index}",
        )
    return None


def _insert_monitoring_transport_costs(db: dbTolls, data: tuple):
    """Вставляет новую запись в таблицу tblMonitoringMaterials из data."""
    message = f"INSERT tblMonitoringTransportCosts: {data[:-1]!r}"
    db.go_insert(
        sql_monitoring_transport_costs["insert_monitoring_transport_costs"],
        data,
        message,
    )


def _make_data_from_raw_monitoring_transport_costs(
    db: dbTolls, raw_line: sqlite3.Row, period: Period
) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с записью monitoring transport cost.
    Возвращает кортеж с данными для вставки в таблицу tblMonitoringTransportCosts.
    Период id ищется по ...
    """
    # FK_tblMonitoringTransportCosts_tblProducts, FK_tblMonitoringTransportCosts_tblPeriods,
    # actual_price,

    code = clear_code(raw_line["code"])
    # тип периода: мониторинг
    monitoring_origin_id = get_origin_id(db, origin_name=MONITORING_ORIGIN)
    origin_message = f"для типа справочника {MONITORING_ORIGIN!r}"
    #
    real_period = get_period_by_origin_and_numbers(db, monitoring_origin_id, period)
    if not real_period:
        output_message_exit(
            f"период {period} не найден в таблице tblPeriods",
            origin_message,
        )
    period_id = real_period["ID_tblPeriod"]
    #
    ton_origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
    real_product = get_product_by_code(db, ton_origin_id, code)
    if not real_product:
        output_message_exit(
            f"Продукт {code} не найден в таблице tblProducts",
            f"период {period} { origin_message}",
        )
    product_id = real_product["ID_tblProduct"]
    actual_price = raw_line["current_price"]
    data = (product_id, period_id, actual_price, period)
    return data

def delete_last_period_monitoring_transport_cost(db_file: str):
    """Удаляет записи из таблицы tblMonitoringMaterials у которых период < максимального.
    Вычисляет максимальный период для таблицы tblMonitoringMaterials."""
    with dbTolls(db_file) as db:
        work_cursor = db.go_execute(
            sql_monitoring_transport_costs[
                "select_monitoring_transport_costs_max_index_number"
            ]
        )
        result = work_cursor.fetchone() if work_cursor else None
        if result is None:
            output_message_exit(
                "Ошибка при получении максимального Номера Индекса периода",
                "Мониторинга Транспортных Расходов",
            )
            return None
        max_index = result["max_index"]
        ic(max_index)
        count_records_to_be_deleted = db.go_execute(
            sql_monitoring_transport_costs[
                "select_monitoring_transport_costs_count_less_index_number"
            ],
            (max_index,),
        )
        number = count_records_to_be_deleted.fetchone()["number"]
        if number > 0:
            # message = f"Будут удалены {number} продуктов с периодом меньше: {max_supplement_number} {query_info}"
            # ic(message)
            deleted_cursor = db.go_execute(
                sql_monitoring_transport_costs[
                    "delete_monitoring_transport_costs_less_max_idex"
                ],
                (max_index,),
            )
            message = f"удалено {deleted_cursor.rowcount} записей с период дополнения < {max_index}"
            ic(message)


def transfer_raw_monitoring_transport_costs(db_file, period: Period):
    """
    Заполняет таблицу tblMonitoringTransportCosts данными из таблицы tblRawData
    только для периода period.
    """
    with dbTolls(db_file) as db:
        raw_data_mtc = get_raw_data(db)
        inserted_success, updated_success = [], []
        for line in raw_data_mtc:
            raw_data = _make_data_from_raw_monitoring_transport_costs(db, line, period)
            # ic(raw_data)
            product_id = raw_data[0]
            transport_cost = db.go_select(
                sql_monitoring_transport_costs[
                    "select_monitoring_transport_costs_by_product"
                ],
                (product_id,),
            )
            if transport_cost:
                _update_monitoring_transport_costs(db, transport_cost[0], raw_data)
                updated_success.append(raw_data)
            else:
                _insert_monitoring_transport_costs(db, raw_data[:-1])
                inserted_success.append(raw_data)
            # db.connection.commit()
        ic(len(raw_data_mtc), len(inserted_success), len(updated_success))
    # удалить записи старых периодов
    delete_last_period_monitoring_transport_cost(db_file)


def _parsing_monitoring_transport_costs(
    location: LocalData, monitoring_transport_costs_csv_file: str, period: Period
) -> int:
    """
    Загружает данные из CSV файла в tblRawData.
    Заполняет tblMonitoringTransportCosts мониторинга транспортных расходов.
    Для периода типа 'мониторинг'.
    """
    print()
    message = f"===>>> Загружаем Мониторинг цен на Транспортные услуги для периода: {period.index} {period.supplement}"
    ic(message)
    file = create_abspath_file(
        location.monitoring_path, monitoring_transport_costs_csv_file
    )
    load_csv_to_raw_table(file, location.db_file, delimiter=",")
    _round_raw_current_price(location.db_file, ROUNDING)
    #
    transfer_raw_monitoring_transport_costs(location.db_file, period)
    return 0


if __name__ == "__main__":
    # from tools.create.create_tables import _create_monitoring_transport_costs_environment

    local = LocalData("office")  # office  # home

    # with dbTolls(local.db_file) as db:
    #     _create_monitoring_transport_costs_environment(db)

    monitoring_period = Period(71, 208)
    data_file = "transport_monitoring_result_71_208.csv"
    _parsing_monitoring_transport_costs(local, data_file, monitoring_period)

    # files = [
    #     ("transport_monitoring_result_71_208.csv", Period(71, 208)),
    #     ("transport_monitoring_result_71_209.csv", Period(71, 209)),
    #     ("transport_monitoring_result_71_210.csv", Period(71, 210)),
    #     ("transport_monitoring_result_72_211.csv", Period(72, 211)),
    # ]
    # for file in files:
    #     _parsing_monitoring_transport_costs(local, file[0], file[1])
