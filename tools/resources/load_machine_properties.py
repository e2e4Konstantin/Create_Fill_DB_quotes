import sqlite3
from icecream import ic

from config import dbTolls, LocalData, TON_ORIGIN
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from sql_queries import (
    sql_raw_queries,
    sql_periods_queries,
    sql_transport_costs,
    sql_storage_costs_queries,
    sql_materials,
)
from files_features import output_message_exit
from tools.shared.code_tolls import (
    clear_code,
    get_float_value,
    get_integer_value,
    text_cleaning,
)
from tools.shared.shared_features import get_product_by_code, get_origin_id


def _get_raw_transport_costs(db: dbTolls) -> list[sqlite3.Row] | None:
    """
    Выбрать все записи Свойства Материалов из таблицы tblRawData отсортированные по индексу (index_number).
    """
    try:
        results = db.go_select(sql_raw_queries["select_rwd_all_sorted_by_index_number"])
        if not results:
            output_message_exit(
                "в RAW таблице с Транспортными Расходами нет записей:",
                "tblRawData пустая",
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
        ic(repr(err))
        return None
    except KeyError as err:
        ic(repr(err))
        return None
    except Exception as err:
        ic(err)
        return None



def _get_transport_cost_by_basic_normative_id(
    db: dbTolls, transport_cost_normative_id: int
) -> int | None:
    """
    Выбрать id транспортных затрат  по transport_cost_normative_id
    """
    try:
        results = db.go_select(
            sql_transport_costs["select_transport_cost_by_base_id"],
            (transport_cost_normative_id,),
        )
        if not results:
            return None
        return results[0]
    except TypeError as err:
        ic(repr(err))
        return None
    except KeyError as err:
        ic(repr(err))
        return None
    except Exception as err:
        ic(err)
        return None


def _get_storage_cost_by_basic_normative_id(
    db: dbTolls, storage_cost_normative_id: int
) -> int | None:
    """
    Выбрать id складских затрат по transport_cost_normative_id
    """
    try:
        results = db.go_select(
            sql_storage_costs_queries["select_storage_costs_by_base_id"],
            (storage_cost_normative_id,),
        )
        if not results:
            return None
        return results[0]
    except TypeError as err:
        ic(repr(err))
        return None
    except KeyError as err:
        ic(repr(err))
        return None
    except Exception as err:
        ic(err)
        return None


def _make_data_from_raw_machine_properties(
    db: dbTolls, raw_machine: sqlite3.Row
) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с записью Normative.resources.machines.
    Возвращает кортеж с данными для вставки в таблицу tblMaterials.
    Период id ищется по id периода из Normative.Periods (в таблице периодов tblPeriods есть поле "basic_id").
    Данные по таблицам Транспортные расходы, Затраты на хранение и Свойства машин заносятся по периодам.
    Поэтому ищем в текущем периоде а не в истории.
    """
    # FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods, FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
    # description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate, base_price, actual_price, estimate_price
    #
    code = clear_code(raw_machine["pressmark"])
    origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
    product_id = get_product_by_code(db, origin_id, code)["ID_tblProduct"]
    period = _get_period_by_basic_normative_id(db, raw_machine["period_id"])
    index_num = period["index_num"]
    period_id = period["ID_tblPeriod"]
    #
    transport_cost = _get_transport_cost_by_basic_normative_id(
        db, raw_machine["transport_cost_id"]
    )
    if transport_cost:
        transport_cost_id = transport_cost["ID_tblTransportCost"]
    else:
        transport_cost_id = None
        output_message_exit(
            "не найдена запись транспортных затрат", f"для {code=}, период: {index_num=}"
        )

    storage_cost = _get_storage_cost_by_basic_normative_id(
        db, raw_machine["storage_cost_id"]
    )
    if storage_cost:
        storage_cost_id = storage_cost["ID_tblStorageCost"]
    else:
        storage_cost_id = None
        output_message_exit(
            "не найдена запись складских расходов",
            f"для {code=}, период: {index_num=}",
        )

    cmt = text_cleaning(raw_machine["cmt"]) if text_cleaning(raw_machine['cmt']) else ""
    long_title = text_cleaning(raw_machine['long_title']) if text_cleaning(raw_machine['long_title']) else ""

    description = f"{cmt} {long_title}"

    rpc = text_cleaning(raw_machine["okp"])
    rpca2 = text_cleaning(raw_machine["okpd2"])
    net_weight = get_float_value(raw_machine["netto"])
    gross_weight = get_float_value(raw_machine["brutto"])
    used_to_calc = get_integer_value(raw_machine["use_to_calc_avg_rate"])

    base_price = get_float_value(raw_machine["price"])
    actual_price = get_float_value(raw_machine["cur_sale_price"])
    estimate_price = get_float_value(raw_machine["cur_price"])

    # FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods, FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
    # description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate, base_price, actual_price, estimate_price
    #
    data = (
        product_id, period_id, transport_cost_id, storage_cost_id,
        description, rpc, rpca2, net_weight, gross_weight, used_to_calc,
        base_price, actual_price, estimate_price,
        index_num,
    )
    return data


def delete_last_period_machine_properties(db_file: str):
    """Удаляет записи из таблицы tblMaterials у которых период < максимального.
    Вычисляет максимальный период для таблицы tblMaterials."""
    with dbTolls(db_file) as db:
        work_cursor = db.go_execute(sql_materials["select_materials_max_index_number"])
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
            sql_materials["select_materials_count_less_index_number"], (max_index,))

        number = count_records_to_be_deleted.fetchone()["number"]
        if number > 0:
            # message = f"Будут удалены {number} продуктов с периодом меньше: {max_supplement_number} {query_info}"
            # ic(message)
            deleted_cursor = db.go_execute(
                sql_materials["delete_materials_less_max_idex"], (max_index,)
            )
            message = f"удалено {deleted_cursor.rowcount} записей с период дополнения < {max_index}"
            ic(message)


def transfer_raw_machine_properties(db_file):
    """
    Заполняет таблицу tblMachines данными из RAW таблицы tblRawData.
    """
    with dbTolls(db_file) as db:
        raw_properties = _get_raw_transport_costs(db)
        inserted_success, updated_success = [], []
        for line in raw_properties:
            # обрабатываем запись raw таблицы
            raw_data = _make_data_from_raw_machine_properties(db, line)
            ic(raw_data)
            raw_index_num = raw_data[-1]
            # ищем такую же запись в tblMaterials
            product_id = raw_data[0]
            material_line = db.go_select(
                sql_materials["select_material_by_product_id"],
                (product_id,),
            )
            if material_line:
                # update
                index_num = material_line[0]["index_num"]
                if raw_index_num >= index_num:
                    data = (*raw_data[:-1], material_line[0]["ID_tblMaterial"])
                    db.go_execute(sql_materials["update_material_by_id"], data)
                    updated_success.append(data)
                    print(data)
                else:
                    output_message_exit(
                        f"Ошибка обновления 'Свойств Материала': {tuple(material_line[0])}",
                        f"номер индекса {index_num} больше загружаемого {raw_index_num}",
                    )
            else:
                # insert
                message = f"INSERT tblMaterials: {raw_data[:-1]!r}"
                db.go_insert(sql_materials["insert_material"], raw_data[:-1], message)
                inserted_success.append(raw_data)
                # print(raw_data)
            # db.connection.commit()
        ic(len(raw_properties), len(inserted_success), len(updated_success))
    #  удалить записи старых периодов
    delete_last_period_machine_properties(db_file)


def parsing_load_machine_properties(location: LocalData) -> int:
    """
    Заполняет tblMaterials свойствами материалов. Читает данные из CSV файла в tblRawData.
    Добавляет столбец index_number в tblRawData. Переносит данные из tblRawData в tblMaterials.
    """
    print()
    ic("===>>> Загружаем Свойства Материалов.")

    # load_csv_to_raw_table(
    #     location.machine_properties_file, location.db_file, delimiter=","
    # )
    # with dbTolls(location.db_file) as db:
    #     db.go_execute(sql_raw_queries["add_index_number_column"])
    #     db.go_execute(sql_raw_queries["update_index_number"])

    transfer_raw_machine_properties(location.db_file)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home

    parsing_load_machine_properties(local)