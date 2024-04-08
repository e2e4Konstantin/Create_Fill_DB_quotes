
import sqlite3
from icecream import ic

from config import dbTolls, LocalData, TON_ORIGIN
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from files_features import create_abspath_file
from sql_queries import sql_raw_queries, sql_periods_queries, sql_storage_costs_queries
from files_features import output_message_exit

from tools.shared.shared_features import get_directory_id, get_period_by_id
from tools.shared.code_tolls import text_cleaning, get_float_value, get_integer_value


def _get_raw_storage_costs(db: dbTolls) -> list[sqlite3.Row] | None:
    """
    Выбрать все записи %ЗСР из таблицы tblRawData отсортированные по индексу.
    """
    try:
        results = db.go_select(sql_raw_queries["select_rwd_all_sorted_by_index_number"])
        if not results:
            output_message_exit("в RAW таблице с %ЗСР нет записей:", "tblRawData пустая")
        return results
    except Exception as err:
        output_message_exit(
            "Непредвиденная ошибка при выборке из RAW таблицы с %ЗСР:", repr(err)
        )
        return None



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
    Период id ищется по id периода из Normative.Periods (в боевой таблице периодов есть поле "basic_id").
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
    id_raw_period = get_integer_value(raw_sc["id_period"])
    # FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description
    data = (
        FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description, id_raw_period
    )
    return data



def transfer_raw_storage_cost(db_file):
    """
    Заполняет таблицу tblStorageCosts данными из RAW таблицы tblRawData.
    """
    with dbTolls(db_file) as db:
        raw_storage_costs = _get_raw_storage_costs(db)
        if raw_storage_costs is None:
            return None
        inserted_success, updated_success = [], []
        for line in raw_storage_costs:
            # обрабатываем запись raw таблицы
            raw_data = _make_data_from_raw_storage_cost(db, line)
            # ищем период по raw id
            id_raw_period = raw_data[5]
            period = db.go_select(
                sql_periods_queries["select_period_by_normative_id"], (id_raw_period,)
            )
            if period is None:
                output_message_exit("Не найден период для %ЗСР:", f"период: {id_raw_period}")
            raw_index_num = period[0]["index_num"]
            # ищем такую же запись в tblStorageCost
            item_id = raw_data[0]
            name = raw_data[2]
            storage_cost_line = db.go_select(
                sql_storage_costs_queries["select_storage_costs_item_name"],
                (item_id, name)
            )
            if storage_cost_line:
                # update
                index_num = storage_cost_line[0]["index_num"]
                if raw_index_num >= index_num:
                    data = (*raw_data[:-1], storage_cost_line[0]["ID_tblStorageCosts"])
                    db.go_execute(sql_storage_costs_queries["update_storage_cost"], data)
                    updated_success.append(data)
                    # print(data)
                else:
                    output_message_exit(
                        f"Ошибка обновления %ЗСР: {tuple(storage_cost_line[0])}",
                        f"номер индекса %ЗСР {index_num} больше загружаемого {raw_index_num}",
                    )
            else:
                # insert
                message = f"INSERT tblStorageCosts: {raw_data[:-1]!r}"
                db.go_insert(sql_storage_costs_queries["insert_storage_cost"], raw_data[:-1], message)
                inserted_success.append(raw_data)
                # print(raw_data)
            db.connection.commit()
        ic(len(raw_storage_costs), len(inserted_success), len(updated_success))


def parsing_storage_cost(location: LocalData) -> int:
    """
    Заполняет tblStorageCosts %ЗСР. Читает данные из CSV файла в tblRawData.
    Добавляет столбец index_number в tblRawData.
    Переносит данные из tblRawData в tblStorageCosts.
    """
    print()
    ic("===>>> Загружаем %ЗСР.")

    storage_cost_csv_file = create_abspath_file(
        location.storage_costs_path, location.storage_costs_file
    )
    load_csv_to_raw_table(storage_cost_csv_file, location.db_file, delimiter=",")
    with dbTolls(location. db_file) as db:
        db.go_execute(sql_raw_queries["add_index_number_column"])
        db.go_execute(sql_raw_queries["update_index_number"])

    transfer_raw_storage_cost(location.db_file)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home

    parsing_storage_cost(local)
