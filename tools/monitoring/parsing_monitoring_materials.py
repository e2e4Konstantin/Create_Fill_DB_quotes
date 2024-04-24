import sqlite3
from icecream import ic

from config import dbTolls, LocalData, MONITORING_ORIGIN, Period
from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from sql_queries import (
    sql_raw_queries,
    sql_periods_queries,
    sql_transport_costs,
    sql_storage_costs_queries,
    sql_materials,
    sql_monitoring,
)
from files_features import output_message_exit, create_abspath_file
from tools.shared.code_tolls import (
    clear_code,
    get_float_value,
    get_integer_value,
    text_cleaning,
)
from tools.shared.shared_features import (
    get_product_by_code,
    get_origin_id,
    get_raw_data,
    get_period_by_origin_and_numbers,
    get_period_by_id,
)


def _make_data_from_raw_monitoring_materials(db: dbTolls, raw_line: sqlite3.Row, period: Period) -> tuple | None:
    """
    Получает строку из таблицы tblRawData с записью Normative.resources.machines.
    Возвращает кортеж с данными для вставки в таблицу tblMaterials.
    Период id ищется по id периода из Normative.Periods (в таблице периодов tblPeriods есть поле "basic_id").
    Данные по таблицам Транспортные расходы, Затраты на хранение и Свойства машин заносятся по периодам.
    Поэтому ищем в текущем периоде а не в истории.
    """
    # FK_tblMonitoringMaterial_tblProducts, FK_tblMonitoringMaterial_tblPeriods,
    # supplier_price, delivery, title

    code = clear_code(raw_line["code"])
    origin_id = get_origin_id(db, origin_name=MONITORING_ORIGIN)
    product_id = get_product_by_code(db, origin_id, code)["ID_tblProduct"]
    #
    period = get_period_by_origin_and_numbers(db, origin_id, period)
    period_id = period["ID_tblPeriod"]
    supplier_price = raw_line["supplier_price"]
    delivery = 1 if raw_line["delivery"] == "True" else 0
    title = raw_line["title"]

    data = (product_id, period_id, supplier_price, delivery, title, period)
    return data


def delete_last_period_monitoring_material_properties(db_file: str):
    """Удаляет записи из таблицы tblMonitoringMaterials у которых период < максимального.
    Вычисляет максимальный период для таблицы tblMaterials."""
    with dbTolls(db_file) as db:
        work_cursor = db.go_execute(
            sql_monitoring["select_monitoring_materials_max_index_number"]
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
            sql_monitoring["select_monitoring_materials_count_less_index_number"],
            (max_index,),
        )
        number = count_records_to_be_deleted.fetchone()["number"]
        if number > 0:
            # message = f"Будут удалены {number} продуктов с периодом меньше: {max_supplement_number} {query_info}"
            # ic(message)
            deleted_cursor = db.go_execute(
                sql_monitoring["delete_monitoring_materials_less_max_idexx"],
                (max_index,),
            )
            message = f"удалено {deleted_cursor.rowcount} записей с период дополнения < {max_index}"
            ic(message)


def transfer_raw_monitoring_materials(db_file, period: Period):
    """
    Заполняет таблицу tblMonitoringMaterials данными из таблицы tblRawData
    только для периода period.
    """
    with dbTolls(db_file) as db:
        raw_data_monitoring = get_raw_data(db)
        inserted_success, updated_success = [], []
        for line in raw_data_monitoring:
            raw_data = _make_data_from_raw_monitoring_materials(db, line, period)
            # ic(raw_data)
            product_id = raw_data[0]
            raw_index = raw_data[5].index
            material = db.go_select(
                sql_monitoring["select_monitoring_materials_by_product"], (product_id,)
            )
            if material:
                # update
                material_id = material[0]["ID_tblMonitoringMaterial"]

                material_period = get_period_by_id(
                    db, material["FK_tblMonitoringMaterial_tblPeriods"]
                )
                index_num = material_period[0]["index_num"]
                if raw_index >= index_num:
                    data = (*raw_data[:-1], material_id)
                    db.go_execute(
                        sql_monitoring["update_monitoring_materials_by_id"], data
                    )
                    updated_success.append(data)
                    # print(data)
                else:
                    output_message_exit(
                        f"Ошибка обновления 'Свойств Мониторинга Материала': {tuple(material[0])}",
                        f"номер индекса {index_num} больше загружаемого {raw_index}",
                    )
            else:
                # insert
                message = f"INSERT tblMaterials: {raw_data[:-1]!r}"
                db.go_insert(
                    sql_monitoring["insert_monitoring_materials"],
                    raw_data[:-1],
                    message,
                )
                inserted_success.append(raw_data)
                # print(raw_data)
            # db.connection.commit()
        ic(len(raw_data_monitoring), len(inserted_success), len(updated_success))
    #  удалить записи старых периодов
    delete_last_period_monitoring_material_properties(db_file)




def parsing_monitoring_materials(
    location: LocalData, monitoring_csv_file: str, period: Period) -> int:
    """
    Заполняет tblMaterials свойствами материалов. Читает данные из CSV файла в tblRawData.
    Добавляет столбец index_number в tblRawData. Переносит данные из tblRawData в tblMaterials.
    Переносит данные из tblRawData в tblMaterials только для периода index_period.
    """
    print()
    message = f"===>>> Загружаем Мониторинг цен поставщиков на Материалы для периода: {period.index} {period.supplement}"
    ic(message)
    file = create_abspath_file(location.monitoring_path, monitoring_csv_file)
    load_csv_to_raw_table(file, location.db_file, delimiter=",")

    transfer_raw_monitoring_materials(location.db_file, period)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home

    monitoring_period = Period(71,210)
    data_file = "Мониторинг_Март_2024_210_71_result.csv"
    parsing_monitoring_materials(local, data_file, monitoring_period)