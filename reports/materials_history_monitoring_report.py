import sqlite3
from icecream import ic


from config import dbTolls, LocalData
from reports.sql_materials_report import sql_materials_reports
from reports.sql_temp_tables import sql_slice_materials
from reports.reports_config import Material, PriceHistory

from files_features import output_message_exit, output_message
from tqdm import tqdm


def _fetch_supplement_number(db: dbTolls, index_number: int)-> int:
    result = db.go_select(
        sql_materials_reports[
            "select_ton_supplement_for_index"
        ],
        ({"ton_index_number": index_number}),
    )
    if not result:
        output_message_exit(
            f"для индексного периода: {index_number=}",
            "не найден номер дополнения.",
        )
    return result[0]["supplement_number"]


def _fetch_materials_from_products_history(db: dbTolls, supplement_number: int) -> sqlite3.Row:
    result = db.go_select(
        sql_materials_reports["select_products_from_history"],
        ({"supplement": supplement_number}),
    )
    if not result:
        output_message_exit(
            f"в _tblHistoryProducts для периода дополнения: {supplement_number=}",
            "не найдено ни одной записи.",
        )
    return result

def _fetch_material_properties_from_history(db: dbTolls, index_number: int, product_id: int) -> sqlite3.Row | None:
    """Получение свойств материала для заданного номера индексного периода по истории."""
    material_properties = db.go_select(
        sql_materials_reports["select_history_material_for_period"],
        {"index_number": index_number, 'product_id': product_id},
    )
    if not material_properties:
        output_message_exit(
            f"в _tblHistoryMaterials для индексного периода: {index_number=}",
            "не найдено ни одной записи.",
        )
    return material_properties[0]

def _fetch_all_products_materials_from_history(
    db: dbTolls, supplement_number: int
) -> list[sqlite3.Row]:
    """Из истории продуктов _tblHistoryProducts выбрать только Материалы для дополнения :supplement.
    Кроме 0-й главы.
    Вытягиваем последнее изменение полей."""
    result = db.go_select(
        sql_materials_reports["select_products_from_history"],
        ({"supplement": supplement_number}),
    )
    if not result:
        output_message_exit(
            f"в _tblHistoryProducts для периода дополнения: {supplement_number=}",
            "не найдено ни одной записи.",
        )
    return result

def _fetch_all_properties_material_from_history(db: dbTolls, index_number: int) -> list[sqlite3.Row] | None:
    """Получение свойств материалов для индексного периода по истории."""
    materials_properties = db.go_select(
        sql_materials_reports["select_all_history_material_for_period"],
        {"index_number": index_number},
    )
    if not materials_properties:
        output_message_exit(
            f"в _tblHistoryMaterials для индексного периода: {index_number=}",
            "не найдено ни одной записи.",
        )
    return materials_properties

def _fetch_all_transport_costs_history(
    db: dbTolls, index_number: int, supplement_number: int
) -> list[sqlite3.Row]:
    """Получение транспортных расходов из истории для заданного номера индекса и дополнения."""
    transport_costs = db.go_select(
        sql_materials_reports["select_all_history_transport_cost_for_period"],
        {
            "index_number": index_number,
            "supplement_number": supplement_number,
        },
    )
    if not transport_costs:
        output_message_exit(
            "не найдены транспортные расходы",
            f"для индекс: {index_number}, дополнения: {supplement_number}",
        )
    return transport_costs


def _fetch_transport_cost_history(
    db: dbTolls, transport_cost_id: int, index_number: int, supplement_number: int
) -> sqlite3.Row:
    """Получение данных транспортных расходов из истории по id для заданного номера индекса и дополнения."""
    transport_cost = db.go_select(
        sql_materials_reports["select_history_transport_cost_for_period"],
        {
            "transport_cost_rowid": transport_cost_id,
            "index_number": index_number,
            "supplement_number": supplement_number,
        },
    )
    if not transport_cost:
        output_message_exit(
            f"не найдены транспортные расходы : {transport_cost_id=}",
            f"для индекс: {index_number}, дополнения: {supplement_number}",
        )
    return transport_cost[0]


def _get_data_materials_monitoring_for_period(
    db_file: str, index_number: int, monitoring_index_number: int
) -> list[Material] | None:
    """Получить материалы для дополнения и свойства для номера дополнения и данные мониторинга для номера индекса."""
    materials = []
    with dbTolls(db_file) as db:
        supplement_number = _fetch_supplement_number(db, index_number)
        materials = _fetch_materials_from_products_history(
            db, supplement_number
        )
        ic('материалов:', len(materials))
        for material in tqdm(
            materials[:10], desc="Processing Materials", total=len(materials)
        ):
        # for material in materials:
            # ic(tuple(material), material.keys())
            product_id = material["product_id"]
            # ic(product_id, material["code"])
            properties = _fetch_material_properties_from_history(db, index_number, product_id)
            # ic(tuple(properties), properties.keys())

            transport_cost_id = properties["properties_transport_cost_id"]
            # if transport_cost_id==1:
            #     ic(transport_cost_id)
            transport_cost = _fetch_transport_cost_history(
                db, transport_cost_id, index_number, supplement_number
            )
            # ic(tuple(transport_cost), transport_cost.keys())
            materials.append({**material, **properties, **transport_cost})
    return materials if materials else None


def _fetch_monitoring_materials_history(
    db: dbTolls,
    monitoring_index_number: int,
) -> list[sqlite3.Row]:
    """Получение истории мониторинга материалов для номера индекса мониторинга."""
    monitoring = db.go_select(
        sql_materials_reports["select_history_monitoring_materials_for_index"],
        {"index_number": monitoring_index_number},
    )
    if not monitoring:
        output_message_exit(
            "не найдены данные в истории мониторинга",
            f"для индекса: {monitoring_index_number=}",
        )
        return None
    return monitoring


def _fetch_report( db: dbTolls) -> list[sqlite3.Row]:
    """ Получение Результирующей таблицы из таблиц
        tblSliceMaterials, tblPropertiesMaterials, tblSliceTransportCosts
    """
    result = db.go_select(sql_slice_materials["select_result"])
    if not result:
        output_message_exit("не найдены строки отчета", "отчет пуст.")
    return result





def _save_slice_materials_for_period(
    db_file: str,
    supplement_number: int, index_number: int,
) -> int:
    """."""
    with dbTolls(db_file) as db:
        # supplement_number = _fetch_supplement_number(db, index_number)
        materials = _fetch_all_products_materials_from_history(db, supplement_number)
        ic("материалов:", len(materials))
        db.go_execute(sql_slice_materials["delete_table_slice_materials"])
        db.go_execute(sql_slice_materials["create_table_slice_materials"])
        db.go_execute(sql_slice_materials["create_index_slice_materials"])
        #
        cursor = db.go_execute_many(sql_slice_materials["insert_slice_materials"], materials)
        ic("вставлено материалов",cursor.rowcount)
        #
        properties = _fetch_all_properties_material_from_history(db, index_number,)
        # ic(properties[0].keys())
        ic("свойств материалов:", len(properties))
        db.go_execute(sql_slice_materials["delete_table_properties_materials"])
        db.go_execute(sql_slice_materials["create_table_properties_materials"])
        db.go_execute(sql_slice_materials["create_index_properties_materials"])
        #
        cursor = db.go_execute_many(
            sql_slice_materials["insert_properties_materials"], properties
        )
        ic("вставлено свойств материалов",cursor.rowcount)
        #
        transport_costs = _fetch_all_transport_costs_history(
            db, index_number, supplement_number
        )
        ic("транспортных расходов:", len(transport_costs))
        # ic("транспортных расходов:", transport_costs[0].keys())
        db.go_execute(sql_slice_materials["delete_table_slice_transport_costs"])
        db.go_execute(sql_slice_materials["create_table_slice_transport_costs"])
        db.go_execute(sql_slice_materials["create_index_slice_transportation_costs"])
        #
        cursor = db.go_execute_many(
            sql_slice_materials["insert_slice_transportation_costs"], transport_costs
        )
        ic("вставлено транспортных расходов",cursor.rowcount)
    return 0

def _save_slice_monitoring_for_period( db_file: str, monitoring_index_number: int) -> int:
    """."""
    with dbTolls(db_file) as db:
        monitoring = _fetch_monitoring_materials_history(db, monitoring_index_number)
        ic("мониторинга:", len(monitoring))
        # ic(monitoring[0].keys())
        db.go_execute(sql_slice_materials["delete_table_slice_monitoring_materials"])
        db.go_execute(sql_slice_materials["create_table_slice_monitoring_materials"])
        db.go_execute(sql_slice_materials["create_index_slice_monitoring_materials"])
        #
        cursor = db.go_execute_many(
            sql_slice_materials["insert_slice_monitoring_materials"], monitoring
        )
        ic("вставлено мониторинга:", cursor.rowcount)
    return 0


def _fetch_report_slice_materials_monitoring(db_file: str) -> list[Material] | None:
    """ получить отчет по материалам с мониторингом."""
    report = []
    with dbTolls(db_file) as db:
        report = _fetch_report(db)
        ic("строк в отчете:", len(report))
    return report if report else None


def _check_products_counts(db_file: str) -> bool:
    with dbTolls(db_file) as db:
        monitoring = db.go_select(
            """--sql
            SELECT monitoring_product_id FROM tblSliceMonitoringMaterials"""
        )
        monitoring_id = [x["monitoring_product_id"] for x in monitoring]
        ic(len(monitoring_id))
        monitoring_id = set(monitoring_id)
        ic(len(monitoring_id))

        products = db.go_select(
            """--sql
            SELECT product_id FROM tblSliceMaterials"""
        )
        products_id = [x["product_id"] for x in products]
        ic(len(products_id))
        products_id = set(products_id)
        ic(len(products_id))
        d = monitoring_id.difference(products_id)
        ic(d)


if __name__ == "__main__":
    location = "office"  # office  # home
    local = LocalData(location)
    ic()

    _save_slice_materials_for_period(local.db_file, supplement_number=72, index_number=210)
    # _save_slice_monitoring_for_period(local.db_file, monitoring_index_number=211)

    # _check_products_counts(local.db_file)

    # table = _fetch_report_slice_materials_monitoring(local.db_file)

    # table = _get_data_materials_monitoring_for_period(
    #     local.db_file, index_number=210, monitoring_index_number=211
    # )
    # if table:
    #     ic(len(table))
    #     # ic(table[0].keys())
        # ic(tuple(table[5]))
