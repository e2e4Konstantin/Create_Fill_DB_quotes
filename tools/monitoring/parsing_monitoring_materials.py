import sqlite3
from icecream import ic

from config import dbTolls, LocalData, MONITORING_ORIGIN, TON_ORIGIN, ROUNDING, Period
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from sql_queries import (
    sql_raw_queries,
    sql_periods_queries,
    sql_transport_costs,
    sql_storage_costs_queries,
    sql_materials,
    sql_monitoring_materials,
)
from files_features import output_message_exit, create_abspath_file
from tools.shared.code_tolls import (
    clear_code,
    get_float_value,
    get_integer_value,
    text_cleaning,
    code_to_number,
)
from tools.shared.shared_features import (
    get_product_by_code,
    get_origin_id,
    get_raw_data,
    get_period_by_origin_and_numbers,
    get_period_by_id,
    get_history_product_by_code,
)


def _make_data_from_raw_monitoring_materials(db: dbTolls, raw_line: sqlite3.Row, period: Period) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с записью monitoring material.
    Возвращает кортеж с данными для вставки в таблицу tblMonitoringMaterials.
    Период id ищется по .
    """
    # FK_tblMonitoringMaterial_tblProducts, FK_tblMonitoringMaterial_tblPeriods,
    # supplier_price, delivery, title

    code = clear_code(raw_line["code"])
    # тип периода: мониторинг
    monitoring_origin_id = get_origin_id(db, origin_name=MONITORING_ORIGIN)
    origin_message = f"для типа справочника {MONITORING_ORIGIN!r}"
    #
    real_period = get_period_by_origin_and_numbers(db, monitoring_origin_id, period)
    if not real_period:
        output_message_exit(
            f"период {period} не найден в таблице tblPeriods", origin_message,
        )
    period_id = real_period["ID_tblPeriod"]
    #
    ton_origin_id = get_origin_id(db, origin_name=TON_ORIGIN)
    real_product = get_product_by_code(db, ton_origin_id, code)
    if not real_product:
        # ищем в истории
        real_product = get_history_product_by_code(db, ton_origin_id, code)
        if not real_product:
            output_message_exit(
                f"Продукт: {code} не найден в таблице tblProducts ни в _tblHistoryProducts",
                f"для периода: {period} {origin_message}, {period_id=} {ton_origin_id=}",
            )
    product_id = real_product["ID_tblProduct"]
    #
    supplier_price = raw_line["supplier_price"]
    delivery = 1 if raw_line["delivery"] == "True" else 0
    title = raw_line["title"] if raw_line["title"] else ""

    data = (product_id, period_id, supplier_price, delivery, title, period)
    return data


def delete_last_period_monitoring_material(db_file: str):
    """Удаляет записи из таблицы tblMonitoringMaterials у которых период < максимального.
    Вычисляет максимальный период для таблицы tblMonitoringMaterials."""
    with dbTolls(db_file) as db:
        work_cursor = db.go_execute(
            sql_monitoring_materials["select_monitoring_materials_max_index_number"]
        )
        result = work_cursor.fetchone() if work_cursor else None
        if result is None:
            output_message_exit(
                "Ошибка при получении максимального Номера Индекса периода",
                "Мониторинга Материалов",
            )
            return None
        max_index = result["max_index"]
        ic(max_index)
        count_records_to_be_deleted = db.go_execute(
            sql_monitoring_materials["select_monitoring_materials_count_less_index_number"],
            (max_index,),
        )
        number = count_records_to_be_deleted.fetchone()["number"]
        if number > 0:
            # message = f"Будут удалены {number} продуктов с периодом меньше: {max_supplement_number} {query_info}"
            # ic(message)
            deleted_cursor = db.go_execute(
                sql_monitoring_materials["delete_monitoring_materials_less_max_idex"],
                ({"index_number": max_index}),
            )
            message = f"удалено {deleted_cursor.rowcount} записей с индексом до {max_index=}"
            ic(message)

def _update_monitoring_material(db: dbTolls, material: sqlite3.Row, data: tuple) -> int:
    """ Обновляет материал мониторинга данными из data. """
    material_id = material["ID_tblMonitoringMaterial"]
    material_period = get_period_by_id(
        db, material["FK_tblMonitoringMaterial_tblPeriods"]
    )
    index_num = material_period["index_num"]
    raw_index = data[5].index
    if raw_index >= index_num:
        data = (*data[:-1], material_id)
        db.go_execute(
            sql_monitoring_materials["update_monitoring_materials_by_id"], data
        )
        return 0
    else:
        output_message_exit(
            f"Ошибка обновления 'Мониторинга Материала': {tuple(material)}",
            f"номер индекса {index_num} больше загружаемого {raw_index}",
        )
    return None


def _insert_monitoring_material(db: dbTolls, data: tuple):
    """ Вставляет новый материал мониторинга в tblMonitoringMaterials из data. """
    message = f"INSERT tblMonitoringMaterials: {data[:-1]!r}"
    db.go_insert(
        sql_monitoring_materials["insert_monitoring_materials"], data, message,
    )


def _get_monitoring_raw_data(db: dbTolls) -> list[sqlite3.Row] | None:
    """Выбрать все записи из сырой таблицы отсортированные по digit_code."""
    try:
        rows = db.go_select(sql_raw_queries["select_monitoring_materials_rwd_all"])
    except AttributeError as e:
        output_message_exit("Ошибка при получении данных из RAW таблицы:", repr(e))
        return None
    if rows is None:
        output_message_exit(
            "в RAW таблице не найдено ни одной записи:", "tblRawData пустая."
        )
    return rows


def _transfer_raw_monitoring_materials(db_file, period: Period):
    """
    Заполняет таблицу tblMonitoringMaterials данными из таблицы tblRawData
    только для периода period.
    """
    with dbTolls(db_file) as db:
        raw_data_monitoring = _get_monitoring_raw_data(db)
        inserted_success, updated_success = [], []
        for line in raw_data_monitoring:
            raw_data = _make_data_from_raw_monitoring_materials(db, line, period)
            # ic(raw_data)
            product_id = raw_data[0]
            material = db.go_select(
                sql_monitoring_materials["select_monitoring_materials_by_product"], (product_id,)
            )
            if material:
                _update_monitoring_material(db, material[0], raw_data)
                updated_success.append(raw_data)
            else:
                _insert_monitoring_material(db, raw_data[:-1])
                inserted_success.append(raw_data)
            # db.connection.commit()
        ic(len(raw_data_monitoring), len(inserted_success), len(updated_success))
    #  удалить записи старых периодов
    delete_last_period_monitoring_material(db_file)

def _round_raw_supplier_price(db_file: str, rounding_digits):
    """Округлить до rounding_digits знаков цену продавца."""
    with dbTolls(db_file) as db:
        db.go_execute(
            sql_raw_queries["round_supplier_price_for_monitoring_materials"],
            (rounding_digits,),
        )

def _add_raw_digit_code(db_file: str):
    """Добавить поле digit_code в таблицу tblRawData и заполнить его. """
    with dbTolls(db_file) as db:
        db.go_execute(sql_raw_queries["add_digit_code_column"])
        data = db.go_select(sql_raw_queries["select_rwd_all"])
        for line in data:
            db.go_execute(
                sql_raw_queries["update_digit_code"],
                ({'digit_code': code_to_number(line["code"]), 'id': line['rowid']})
                )


def _parsing_monitoring_materials(
    location: LocalData, monitoring_csv_file: str, period: Period) -> int:
    """
    Заполняет tblMonitoringMaterials мониторинга материалов.
    Для периода типа 'мониторинг' данными из CSV файла прочитанными в tblRawData.
    """
    print()
    message = f"===>>> Загружаем Мониторинг цен поставщиков на Материалы для периода: {period.index} {period.supplement}"
    ic(message)
    file = create_abspath_file(location.monitoring_path, monitoring_csv_file)
    load_csv_to_raw_table(file, location.db_file, delimiter=",")
    _round_raw_supplier_price(location.db_file, ROUNDING)
    _add_raw_digit_code(location.db_file)
    #
    _transfer_raw_monitoring_materials(location.db_file, period)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home

    # monitoring_period = Period(71,210)
    # data_file = "materials_monitoring_result_71_208.csv"
    # _parsing_monitoring_materials(local, data_file, monitoring_period)

    files = [
        ("materials_monitoring_result_71_208.csv", Period(71, 208)),
        ("materials_monitoring_result_71_209.csv", Period(71, 209)),
        ("materials_monitoring_result_71_210.csv", Period(71, 210)),
        ("materials_monitoring_result_72_211.csv", Period(72, 211)),
    ]
    for file in files:  # [2:3]
        _parsing_monitoring_materials(local, file[0], file[1])