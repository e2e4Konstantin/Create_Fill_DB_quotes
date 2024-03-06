from config import dbTolls
from sql_queries import (
    sql_items_creates, sql_catalog_creates, sql_products_creates, sql_raw_queries,
    sql_attributes_queries, sql_options_queries, sql_origins, sql_periods_queries
)


def _create_directory_environment(db: dbTolls):
    """ Создать инфраструктуру для Справочников. Таблицы, индексы и триггеры. """
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

    # --- > Справочник Происхождения

    db.go_execute(sql_origins["delete_table_origins"])
    db.go_execute(sql_origins["delete_index_origins"])
    db.go_execute(sql_origins["create_table_origins"])
    db.go_execute(sql_origins["create_index_origins"])


def _create_catalog_environment(db: dbTolls):
    """ Создать инфраструктуру для Каталога. Таблицы, индексы и триггеры. """
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
    """ Создать инфраструктуру базовой таблицы для хранения
        Расценок, Материалов, Машин и Оборудования.
        Таблицы, индексы, триггеры и представления. """
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


def _create_attributes_environment(db: dbTolls):
    """ Инфраструктура для Атрибутов. """
    db.go_execute(sql_attributes_queries["delete_table_attributes"])
    db.go_execute(sql_attributes_queries["delete_index_attributes"])
    db.go_execute(sql_attributes_queries["delete_view_attributes"])
    db.go_execute(sql_attributes_queries["delete_table_history_attributes"])

    db.go_execute(sql_attributes_queries["create_table_attributes"])
    db.go_execute(sql_attributes_queries["create_index_attributes"])
    db.go_execute(sql_attributes_queries["create_view_attributes"])

    db.go_execute(sql_attributes_queries["create_table_history_attributes"])
    db.go_execute(sql_attributes_queries["create_index_history_attributes"])
    db.go_execute(sql_attributes_queries["create_trigger_history_attributes_insert"])
    db.go_execute(sql_attributes_queries["create_trigger_history_attributes_delete"])
    db.go_execute(sql_attributes_queries["create_trigger_history_attributes_update"])


def _create_options_environment(db: dbTolls):
    """ Инфраструктура для Параметров. """
    db.go_execute(sql_options_queries["delete_table_options"])
    db.go_execute(sql_options_queries["delete_index_options"])
    db.go_execute(sql_options_queries["delete_view_options"])
    db.go_execute(sql_options_queries["delete_table_history_options"])

    db.go_execute(sql_options_queries["create_table_options"])
    db.go_execute(sql_options_queries["create_index_options"])
    db.go_execute(sql_options_queries["create_view_options"])

    db.go_execute(sql_options_queries["create_table_history_options"])
    db.go_execute(sql_options_queries["create_index_history_options"])
    db.go_execute(sql_options_queries["create_trigger_history_options_insert"])
    db.go_execute(sql_options_queries["create_trigger_history_options_delete"])
    db.go_execute(sql_options_queries["create_trigger_history_options_update"])


def _create_periods_environment(db: dbTolls) -> int:
    """ Инфраструктура для Периодов. """
    db.go_execute(sql_periods_queries["delete_table_periods"])
    db.go_execute(sql_periods_queries["delete_table_periods"])
    db.go_execute(sql_periods_queries["create_table_periods"])
    db.go_execute(sql_periods_queries["create_index_periods"])
    db.go_execute(sql_periods_queries["create_view_periods"])
    return 0

# def _create_quotes_chains_environment(db: dbTolls):
#     """ Создать инфраструктуру для Иерархии расценок. Таблицы, индексы и триггеры. """
#     db.go_execute(sql_quotes_chain_create["delete_table_quotes_chains"])
#     db.go_execute(sql_quotes_chain_create["delete_index_quotes_chains"])
#     db.go_execute(sql_quotes_chain_create["delete_quotes_history_quotes_chains"])
#     db.go_execute(sql_quotes_chain_create["delete_index_history_quotes_chains"])
#
#     db.go_execute(sql_quotes_chain_create["create_table_quotes_chains"])
#     db.go_execute(sql_quotes_chain_create["create_index_quotes_chain"])
#     db.go_execute(sql_quotes_chain_create["create_table_history_quotes_chains"])
#     db.go_execute(sql_quotes_chain_create["create_index_history_quotes_chain"])
#
#     db.go_execute(sql_quotes_chain_create["create_trigger_history_quotes_chain_insert"])
#     db.go_execute(sql_quotes_chain_create["create_trigger_history_quotes_chain_delete"])
#     db.go_execute(sql_quotes_chain_create["create_trigger_history_quotes_chain_update"])




def create_tables_indexes(db_file: str):
    """
        Создает таблицы:
            Справочников tblDirectoryItems
            Каталога tblCatalogs
            Расценок, Материалов, Машин и Оборудования tblProducts
            Атрибутов и Параметров продуктов tblAttributes, tblOptions
            Периодов tblPeriods
            > Иерархия расценок -- tblQuotesChains ---
            > Ресурсы -- tblResources ---
    """
    with dbTolls(db_file) as db:
        _create_directory_environment(db)
        _create_catalog_environment(db)
        _create_products_environment(db)
        _create_attributes_environment(db)
        _create_options_environment(db)
        _create_periods_environment(db)

        # _create_quotes_chains_environment(db)
        # _create_resources_environment(db)


def delete_raw_tables(db_file_name: str):
    with dbTolls(db_file_name) as db:
        db.go_execute(sql_raw_queries["delete_table_raw_data"])


def create_index_resources_raw_data(db_file_name: str):
    """ Создать индекс tblRawData когда туда прочитаны Ресурсы (1, 2, 13). """
    with dbTolls(db_file_name) as db:
        db.go_execute(sql_raw_queries["create_index_raw_data"])


if __name__ == '__main__':
    import os
    from icecream import ic
    from data_path import set_data_location

    location = "office"  # office  # home
    ic(location)
    local_path = set_data_location(location)
    
    ic(local_path.__str__())

    # удаляем файл БД если такой есть
    if os.path.isfile(local_path.db_file):
        os.unlink(local_path.db_file)

    # создать таблицы БД
    create_tables_indexes(local_path.db_file)
