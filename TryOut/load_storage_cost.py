
import sqlite3
from icecream import ic

from config import dbTolls, LocalData, TON_ORIGIN
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from files_features import create_abspath_file
from sql_queries import sql_raw_queries, sql_periods_queries
from files_features import output_message_exit

from tools.shared.shared_features import get_directory_id
from tools.shared.code_tolls import text_cleaning, get_float_value


def _get_raw_storage_costs(db: dbTolls) -> list[sqlite3.Row] | None:
    """
    Выбрать все записи %ЗСР из таблицы tblRawData.
    """
    results = db.go_select(sql_raw_queries["select_rwd_all"])
    if not results:
        output_message_exit("в RAW таблице с %ЗСР нет записей:", "tblRawData пустая")
        return None
    return results


def _get_period_id_by_basic_normative_id(db: dbTolls, normative_id: int) -> int | None:
    """
    Выбрать id периода по нужному normative_id
    """
    results = db.go_select(sql_periods_queries["select_period_id_by_normative_id"], (normative_id, ))
    return results[0]["ID_tblPeriod"] if results else None




def _make_data_from_raw_storage_cost(db: dbTolls, raw_sc: sqlite3.Row) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с записью Normative.StorageCost.
    Возвращает кортеж с данными для вставки в таблицу StorageCosts.
    """
    match raw_sc["type"]:
        case "Оборудование":
            item_type = "material"
        case "Материал":
            item_type = "equipment"
        case _ :
            output_message_exit(
                "в RAW таблице с StorageCost не найден тип записи:",
                f"в справочнике {raw_sc['type']}",
            )
    item_id = get_directory_id(db, "units", item_type)
    if item_type is None:
        return None
    FK_tblStorageCosts_tblItems = item_id
    period_id = _get_period_id_by_basic_normative_id(db, raw_sc["id_period"])
    if period_id is None:
        return None
    FK_tblStorageCosts_tblPeriods = period_id
    name =  text_cleaning(raw_sc["title"])
    value = get_float_value(raw_sc["rate"])
    if value and value <= 1e-8:
        percent_storage_costs = 0
    else:
        percent_storage_costs = value
    description = text_cleaning(raw_sc["cmt"])
    # FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description
    data = (
        FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description
    )
    return data


def transfer_raw_storage_cost(db_file):
    with dbTolls(db_file) as db:
        raw_storage_costs = _get_raw_storage_costs(db)
        if raw_storage_costs is None:
            return None
        for line in raw_storage_costs:
            data = _make_data_from_raw_storage_cost(db, line)
            ic(data)




def parsing_storage_cost(location: LocalData) -> int:
    """
    Заполняет tblStorageCosts %ЗСР. Читает данные из CSV файла в tblRawData.
    Переносит данные из tblRawData в tblStorageCosts.
    """
    print()
    ic("===>>> Загружаем %ЗСР.")

    storage_cost_csv_file = create_abspath_file(
        location.storage_costs_path, location.storage_costs_file
    )
    load_csv_to_raw_table(storage_cost_csv_file, location.db_file, delimiter=",")

    transfer_raw_storage_cost(location.db_file)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home

    transfer_raw_storage_cost(local.db_file)
    # parsing_storage_cost(local)
