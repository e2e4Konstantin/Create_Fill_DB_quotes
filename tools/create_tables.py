from config import dbTolls
from sql_queries import sql_items_creates, sql_catalog_creates, sql_bases_creates


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


# def _create_bases_environment(db: dbTolls):
#     """ Создать инфраструктуру базовой таблицы.
#         Для хранения Материалов, Машин и Оборудования.
#         Таблицы, индексы, триггеры и представления. """
#     db.go_execute(sql_bases_creates["delete_table_bases"])
#     db.go_execute(sql_bases_creates["delete_index_bases"])
#     db.go_execute(sql_bases_creates["delete_view_bases"])
#     #
#     db.go_execute(sql_bases_creates["create_table_bases"])
#     db.go_execute(sql_bases_creates["create_index_bases"])
#     db.go_execute(sql_bases_creates["create_view_bases"])
#     #
#     db.go_execute(sql_bases_creates["create_table_history_bases"])
#     db.go_execute(sql_bases_creates["create_index_history_bases"])
#     #
#     db.go_execute(sql_bases_creates["create_trigger_history_bases_insert"])
#     db.go_execute(sql_bases_creates["create_trigger_history_bases_delete"])
#     db.go_execute(sql_bases_creates["create_trigger_history_bases_update"])



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


# def _create_resources_environment(db: dbTolls):
#     """ Создать инфраструктуру для Ресурсов. Главы 1, 2, 13. Таблицы, индексы и триггеры. """
#     db.go_execute(sql_resources_create["delete_table_quotes_chains"])
#     db.go_execute(sql_resources_create["delete_index_quotes_chains"])
#     db.go_execute(sql_resources_create["delete_quotes_history_quotes_chains"])
#     db.go_execute(sql_resources_create["delete_index_history_quotes_chains"])
#
#     db.go_execute(sql_resources_create["create_table_resources"])
#     db.go_execute(sql_resources_create["create_index_resources"])
#     db.go_execute(sql_resources_create["create_table_history_resource"])
#     db.go_execute(sql_resources_create["create_index_history_resources"])
#
#     db.go_execute(sql_resources_create["create_trigger_history_resources_insert"])
#     db.go_execute(sql_resources_create["create_trigger_history_resources_delete"])
#     db.go_execute(sql_resources_create["create_trigger_history_resources_update"])


def create_tables_indexes(db_file_name: str):
    with dbTolls(db_file_name) as db:
        # --- > Справочники -- tblDirectoryItems --
        _create_directory_environment(db)
        # --- > Каталог -- tblCatalogs ---
        _create_catalog_environment(db)


        # # --- > Расценки, Материалы, Машины и Оборудование -- tblBases ---
        # _create_bases_environment(db)

        # # --- > Иерархия расценок -- tblQuotesChains ---
        # _create_quotes_chains_environment(db)
        # # --- > Ресурсы -- tblResources ---
        # _create_resources_environment(db)


if __name__ == '__main__':
    import os
    from icecream import ic

    # db_name = os.path.join(r"F:\Kazak\GoogleDrive\Python_projects\DB", "quotes_test.sqlite3")
    db_name = os.path.join(r"C:\Users\kazak.ke\Documents\PythonProjects\DB", "quotes_test.sqlite3")
    ic(db_name)
    # удаляем файл БД если такой есть
    # if os.path.isfile(db_name):
    #     os.unlink(db_name)

    # создать таблицы БД
    create_tables_indexes(db_name)
