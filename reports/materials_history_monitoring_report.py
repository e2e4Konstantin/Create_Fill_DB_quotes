import sqlite3
from icecream import ic


from config import dbTolls, LocalData
from reports.sql_materials_report import sql_materials_reports
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
            materials, desc="Processing Materials", total=len(materials)
        ):



        # for material in materials:
            # ic(tuple(material), material.keys())
            product_id = material["material_product_id"]
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


if __name__ == "__main__":
    location = "office"  # office  # home
    local = LocalData(location)
    ic()

    table = _get_data_materials_monitoring_for_period(
        local.db_file, index_number=210, monitoring_index_number=211
    )
    if table:
        ic(len(table))
        # ic(table[0].keys())
        # ic(tuple(table[5]))
