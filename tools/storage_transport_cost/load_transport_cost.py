import sqlite3
from icecream import ic

from config import dbTolls, LocalData, TON_ORIGIN
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from tools.shared.code_tolls import (
    clear_code,
    get_float_value,
    get_integer_value,
    text_cleaning,
)
from tools.shared.shared_features import get_product_by_code, get_origin_id
from sql_queries import sql_raw_queries, sql_periods_queries, sql_transport_costs
from files_features import output_message_exit



def _get_raw_transport_costs(db: dbTolls, normative_period_id: int) -> list[sqlite3.Row] | None:
    """
    Выбрать все записи %ЗСР из таблицы tblRawData отсортированные по индексу.
    """
    try:
        results = db.go_select(
            sql_raw_queries["select_rwd_for_normative_period_id"],
            (normative_period_id,),
        )
        if not results:
            output_message_exit(
                "в RAW таблице с Транспортными Расходами нет записей:", "tblRawData пустая"
            )
        return results
    except Exception as err:
        output_message_exit(
            "Непредвиденная ошибка при выборке из RAW таблицы с Транспортными Расходами:",
            repr(err),
        )
        return None

def _get_period_by_basic_normative_id(db: dbTolls, normative_id: int) -> int | None:
    """
    Выбрать id периода по нужному normative_id
    """
    try:
        results = db.go_select(
            sql_periods_queries["select_period_by_normative_id"], (normative_id,)
        )
        if not results:
            return None
        return results[0]
    except TypeError as err:
        # results is None
        return None
    except KeyError as err:
        # results[0]["ID_tblPeriod"] is missing
        ic(repr(err))
        return None
    except Exception as err:
        # unhandled exception
        ic(err)
        return None


def _make_data_from_raw_transport_cost(db: dbTolls, raw_trans_cost: sqlite3.Row) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с записью Normative.TransportCost.
    Возвращает кортеж с данными для вставки в таблицу TransportCosts.
    Период id ищется по id периода из Normative.Periods (в таблице периодов tblPeriods есть поле "basic_id").
    """
    code = clear_code(raw_trans_cost["pressmark"])
    origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
    FK_tblTransportCosts_tblProducts = get_product_by_code(db, origin_id, code)[
        "ID_tblProduct"
    ]
    period = _get_period_by_basic_normative_id(
        db, raw_trans_cost["id_period"]
    )
    index_num = period["index_num"]
    FK_tblTransportCosts_tblPeriods = period["ID_tblPeriod"]
    base_price = get_float_value(raw_trans_cost["price"])
    actual_price = get_float_value(raw_trans_cost["cur_price"])
    numeric_ratio = get_float_value(raw_trans_cost["ratio"])

    base_normative_id = get_integer_value(raw_trans_cost["id"])

    # FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods,
    # base_price, actual_price, numeric_ratio, description
    data = (
        FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods,
        base_price, actual_price, numeric_ratio, base_normative_id,
        index_num
    )
    return data


def delete_last_period_transport_cost(db_file: str):
    """
    Вычисляет максимальный период для таблицы tblStorageCosts.
    Удаляет записи из таблицы tblStorageCosts у которых период < максимального.
    """
    with dbTolls(db_file) as db:
        work_cursor = db.go_execute(
            sql_storage_costs_queries["select_storage_costs_max_index_number"]
        )
        result = work_cursor.fetchone() if work_cursor else None
        if result is None:
            output_message_exit(
                "Ошибка при получении максимального Номера Индекса периода",
                "Свойств Материалов",
            )
            return None
        max_index = result["max_index"]
        ic(max_index)

        count_records_to_be_deleted = db.go_execute(
            sql_storage_costs_queries["select_storage_costs_count_less_index_number"],
            (max_index,),
        )

        number = count_records_to_be_deleted.fetchone()["number"]
        if number > 0:
            # message = f"Будут удалены {number} продуктов с периодом меньше: {max_supplement_number} {query_info}"
            # ic(message)
            deleted_cursor = db.go_execute(
                sql_storage_costs_queries["delete_storage_costs_less_max_idex"],
                (max_index,),
            )
            message = f"удалено {deleted_cursor.rowcount} записей с период дополнения < {max_index}"
            ic(message)


def transfer_raw_transport_cost(db_file, normative_index_period_id: int):
    """
    Заполняет таблицу tblTransportCosts данными из RAW таблицы tblRawData.
    только для периода normative_index_period_id
    """
    with dbTolls(db_file) as db:
        raw_transport_costs = _get_raw_transport_costs(db, normative_index_period_id)
        inserted_success, updated_success = [], []
        for line in raw_transport_costs:
            # обрабатываем запись raw таблицы
            raw_data = _make_data_from_raw_transport_cost(db, line)
            raw_index_num = raw_data[-1]
            # ищем такую же запись в tblTransportCost
            product_id = raw_data[0]
            transport_cost_line = db.go_select(
                sql_transport_costs["select_transport_cost_by_product_id"], (product_id, )
            )
            if transport_cost_line:
                # update
                index_num = transport_cost_line[0]["index_num"]
                if raw_index_num >= index_num:
                    data = (
                        *raw_data[:-1],
                        transport_cost_line[0]["ID_tblTransportCost"],
                    )
                    db.go_execute(sql_transport_costs["update_transport_cost_id"], data)
                    updated_success.append(data)
                    # print(data)
                else:
                    output_message_exit(
                        f"Ошибка обновления 'Транспортных расходов': {tuple(transport_cost_line[0])}",
                        f"номер индекса ТР {index_num} больше загружаемого {raw_index_num}",
                    )
            else:
                # insert
                message = f"INSERT tblTransportCosts: {raw_data[:-1]!r}"
                db.go_insert(
                    sql_transport_costs["insert_transport_cost"],
                    raw_data[:-1],
                    message,
                )
                inserted_success.append(raw_data)
                # print(raw_data)
            # db.connection.commit()
        ic(len(raw_transport_costs), len(inserted_success), len(updated_success))


def parsing_transport_cost(location: LocalData, normative_index_period_id: int) -> int:
    """
    Заполняет tblStorageCosts %ЗСР. Читает данные из CSV файла в tblRawData.
    Добавляет столбец index_number в tblRawData.
    Переносит данные из tblRawData в tblStorageCosts только для периода normative_index_period_id.
    """
    print()
    ic("===>>> Загружаем Транспортные расходы.")

    load_csv_to_raw_table(location.transport_costs_file, location.db_file, delimiter=",")
    with dbTolls(location.db_file) as db:
        db.go_execute(sql_raw_queries["add_index_number_column"])
        db.go_execute(sql_raw_queries["update_index_number"])

    transfer_raw_transport_cost(location.db_file, normative_index_period_id)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home

    parsing_transport_cost(local)