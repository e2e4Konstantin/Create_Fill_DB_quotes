import os
from config import MAIN_RECORD_CODE, TON_ORIGIN, PNWC_ORIGIN
from config import dbTolls
from sql_queries import (
    sql_items_creates,
    sql_catalog_creates,
    sql_products_creates,
    sql_origins,
    sql_periods_queries,
    sql_storage_costs_queries,
    sql_transport_costs,
    sql_materials,
    sql_monitoring_materials,
    sql_monitoring_transport_costs,
)

from tools.create.fill_directory import (
    fill_directory_origins,
    fill_directory_catalog_items,
)
from tools.create.insert_root_row_catalog import insert_root_record_to_catalog
from TryOut.insert_default_value import insert_default_value_record_to_product



def _create_directory_environment(db: dbTolls):
    """Создать инфраструктуру для Справочников. Таблицы, индексы и триггеры."""
    db.go_execute(sql_items_creates["delete_table_items"])
    db.go_execute(sql_items_creates["delete_index_items"])
    db.go_execute(sql_items_creates["delete_table_items_history"])
    db.go_execute(sql_items_creates["delete_index_items_history"])
    # -- tblDirectoryItems --
    db.go_execute(sql_items_creates["create_table_items"])
    db.go_execute(sql_items_creates["create_index_items"])
    #
    db.go_execute(sql_items_creates["create_table_history_items"])
    db.go_execute(sql_items_creates["create_index_history_items"])
    #
    db.go_execute(sql_items_creates["create_trigger_history_items_insert"])
    db.go_execute(sql_items_creates["create_trigger_history_items_delete"])
    db.go_execute(sql_items_creates["create_trigger_history_items_update"])
    #
    # --- > Справочник Происхождения
    #
    db.go_execute(sql_origins["delete_table_origins"])
    db.go_execute(sql_origins["delete_index_origins"])
    db.go_execute(sql_origins["create_table_origins"])
    db.go_execute(sql_origins["create_index_origins"])


def _create_catalog_environment(db: dbTolls):
    """Создать инфраструктуру для Каталога. Таблицы, индексы и триггеры."""
    db.go_execute(sql_catalog_creates["delete_table_catalog"])
    db.go_execute(sql_catalog_creates["delete_index_catalog"])
    db.go_execute(sql_catalog_creates["delete_view_catalog"])
    db.go_execute(sql_catalog_creates["delete_table_catalog_history"])
    db.go_execute(sql_catalog_creates["delete_index_catalog_history"])
    #
    db.go_execute(sql_catalog_creates["create_table_catalogs"])
    db.go_execute(sql_catalog_creates["create_index_catalog"])
    db.go_execute(sql_catalog_creates["create_view_catalog"])
    #
    db.go_execute(sql_catalog_creates["create_table_history_catalog"])
    db.go_execute(sql_catalog_creates["create_index_history_catalog"])
    #
    db.go_execute(sql_catalog_creates["create_trigger_history_catalog_insert"])
    db.go_execute(sql_catalog_creates["create_trigger_history_catalog_delete"])
    db.go_execute(sql_catalog_creates["create_trigger_history_catalog_update"])


def _create_products_environment(db: dbTolls):
    """Создать инфраструктуру базовой таблицы для хранения
    Расценок, Материалов, Машин и Оборудования.
    Таблицы, индексы, триггеры и представления."""
    db.go_execute(sql_products_creates["delete_table_products"])
    db.go_execute(sql_products_creates["delete_index_products"])
    db.go_execute(sql_products_creates["delete_view_products"])
    #
    db.go_execute(sql_products_creates["delete_table_products_history"])
    db.go_execute(sql_products_creates["delete_index_products_history"])
    #
    db.go_execute(sql_products_creates["create_table_products"])
    db.go_execute(sql_products_creates["create_index_products"])
    db.go_execute(sql_products_creates["create_view_products"])
    #
    db.go_execute(sql_products_creates["create_table_history_products"])
    db.go_execute(sql_products_creates["create_index_history_products"])
    #
    db.go_execute(sql_products_creates["create_trigger_history_products_insert"])
    db.go_execute(sql_products_creates["create_trigger_history_products_delete"])
    db.go_execute(sql_products_creates["create_trigger_history_products_update"])


def _create_periods_environment(db: dbTolls) -> int:
    """Инфраструктура для Периодов."""
    db.go_execute(sql_periods_queries["delete_table_periods"])
    db.go_execute(sql_periods_queries["delete_table_periods"])
    db.go_execute(sql_periods_queries["create_table_periods"])
    db.go_execute(sql_periods_queries["create_index_periods"])
    db.go_execute(sql_periods_queries["create_view_periods"])
    return 0


def _create_storage_costs_environment(db: dbTolls) -> int:
    """
    Создать инфраструктуру для %ЗСР. % Заготовительно-складских расходов (%ЗСР)
    """
    db.go_execute(sql_storage_costs_queries["delete_table_storage_costs"])
    db.go_execute(sql_storage_costs_queries["delete_view_storage_costs"])
    db.go_execute(sql_storage_costs_queries["delete_table_history_storage_costs"])
    #
    db.go_execute(sql_storage_costs_queries["create_table_storage_costs"])
    db.go_execute(sql_storage_costs_queries["create_index_purchasing_storage_costs"])
    #
    db.go_execute(sql_storage_costs_queries["create_table_history_storage_costs"])
    db.go_execute(sql_storage_costs_queries["create_index_history_storage_costs"])
    #
    db.go_execute(sql_storage_costs_queries["create_trigger_insert_storage_costs"])
    db.go_execute(sql_storage_costs_queries["create_trigger_delete_storage_costs"])
    db.go_execute(sql_storage_costs_queries["create_trigger_update_storage_cost"])

    db.go_execute(sql_storage_costs_queries["create_view_storage_costs"])
    return 0


def _create_transport_costs_environment(db):
    """
    Создать инфраструктуру для хранения транспортных расходов
    """
    db.go_execute(sql_transport_costs["delete_table_transport_costs"])
    db.go_execute(sql_transport_costs["delete_view_transport_costs"])
    db.go_execute(sql_transport_costs["delete_table_history_transport_costs"])

    db.go_execute(sql_transport_costs["create_table_transport_costs"])
    db.go_execute(sql_transport_costs["create_index_transport_costs"])
    db.go_execute(sql_transport_costs["create_view_transport_costs"])

    db.go_execute(sql_transport_costs["create_table_history_transport_costs"])
    db.go_execute(sql_transport_costs["create_index_history_transport_costs"])
    db.go_execute(sql_transport_costs["create_trigger_insert_transport_costs"])
    db.go_execute(sql_transport_costs["create_trigger_delete_transport_costs"])
    db.go_execute(sql_transport_costs["create_trigger_update_transport_costs"])


def _create_material_properties_environment(db):
    """ Создать инфраструктуру для хранения свойств материалов """
    db.go_execute(sql_materials["delete_table_materials"])
    db.go_execute(sql_materials["delete_view_materials"])
    db.go_execute(sql_materials["delete_table_history_materials"])
    #
    db.go_execute(sql_materials["create_table_materials"])
    db.go_execute(sql_materials["create_index_materials"])
    db.go_execute(sql_materials["create_view_materials"])
    #
    db.go_execute(sql_materials["create_table_history_materials"])
    db.go_execute(sql_materials["create_index_history_materials"])
    #
    db.go_execute(sql_materials["create_trigger_insert_materials"])
    db.go_execute(sql_materials["create_trigger_delete_materials"])
    db.go_execute(sql_materials["create_trigger_update_materials"])


def _create_monitoring_material_environment(db):
    """Создать инфраструктуру для хранения мониторинга материалов"""
    queries = [
        "delete_table_monitoring_materials",
        "delete_table_history_monitoring_materials",
        "delete_view_monitoring_materials",
        "create_table_monitoring_materials",
        "create_index_monitoring_materials",
        "create_view_monitoring_materials",
        "create_table_history_monitoring_materials",
        "create_index_history_monitoring_materials",
        "create_trigger_history_monitoring_materials_insert",
        "create_trigger_history_monitoring_materials_delete",
        "create_trigger_history_monitoring_materials_update",
    ]
    for query in queries:
        db.go_execute(sql_monitoring_materials[query])

def _create_monitoring_transport_costs_environment(db):
    """Создать инфраструктуру для хранения мониторинга материалов"""
    queries = [
        "delete_table_monitoring_transport_costs",
        "delete_table_history_monitoring_transport_costs",
        "delete_view_monitoring_transport_costs",
        #
        "create_table_monitoring_transport_costs",
        "create_index_monitoring__transport_costs",
        "create_view_monitoring_transport_costs",
        #
        "create_table_history_monitoring_transport_costs",
        "create_index_history_monitoring_transport_costs",
        "create_trigger_history_monitoring_transport_costs_insert",
        "create_trigger_history_monitoring_transport_costs_delete",
        "create_trigger_history_monitoring_transport_costs_update",
    ]
    for query in queries:
        db.go_execute(sql_monitoring_transport_costs[query])


def create_tables_indexes(db_file: str):
    """
    Создает таблицы:
        Справочников tblDirectoryItems
        Каталога tblCatalogs
        Расценок, Материалов, Машин и Оборудования tblProducts
        Периодов tblPeriods
        > Ресурсы -- tblResources ---
    """
    # создает инфраструктуру
    with dbTolls(db_file) as db:
        _create_directory_environment(db)
        _create_catalog_environment(db)
        _create_products_environment(db)
        _create_periods_environment(db)
        _create_storage_costs_environment(db)
        _create_transport_costs_environment(db)
        _create_material_properties_environment(db)
        # monitoring
        _create_monitoring_material_environment(db)
        _create_monitoring_transport_costs_environment(db)


def db_create_tables_and_fill_directory(db_file: str) -> int:
    """
    Удаляет файл БД если такой есть. Создает таблицы, индексы, триггеры.
    Заполняет справочник происхождения tblOrigins, справочник элементов каталога.
    Вставляет корневую запись для ТСН. Вставляет корневую запись в каталог для НЦКР.
    """
    if os.path.isfile(db_file):
        os.unlink(db_file)

    create_tables_indexes(db_file)
    fill_directory_origins(db_file)
    fill_directory_catalog_items(db_file)
    insert_root_record_to_catalog(
        db_file,
        catalog=TON_ORIGIN,
        code=MAIN_RECORD_CODE,
        period=0,
        description="Справочник нормативов ТСН",
    )
    #
    insert_root_record_to_catalog(
        db_file,
        catalog=PNWC_ORIGIN,
        code=MAIN_RECORD_CODE,
        period=0,
        description="Справочник ресурсов НЦКР",
    )
    return 0


if __name__ == "__main__":
    from config import LocalData

    location = "office"  # office  # home
    local = LocalData(location)

    db_create_tables_and_fill_directory(local.db_file)


