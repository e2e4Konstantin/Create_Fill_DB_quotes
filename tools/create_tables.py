from config import dbTolls
from sql_queries import sql_directory_creates, sql_catalog_creates, sql_quotes_creates, sql_quotes_chain_create


def _create_directory_environment(db: dbTolls):
    """ Создать инфраструктуру для Справочников. Таблицы, индексы и триггеры. """
    db.go_execute(sql_directory_creates["delete_table_directory"])
    db.go_execute(sql_directory_creates["delete_index_directory"])
    db.go_execute(sql_directory_creates["create_table_directory_items"])
    db.go_execute(sql_directory_creates["create_index_director_items"])
    # -- tblDirectoryItems --
    db.go_execute(sql_directory_creates["delete_table_history_directory"])
    db.go_execute(sql_directory_creates["delete_index_history_directory"])
    db.go_execute(sql_directory_creates["delete_trigger_history_directory_items_insert"])
    db.go_execute(sql_directory_creates["delete_trigger_history_directory_items_delete"])
    db.go_execute(sql_directory_creates["delete_trigger_history_directory_items_update"])
    #
    db.go_execute(sql_directory_creates["create_table_history_directory_items"])
    db.go_execute(sql_directory_creates["create_index_history_directory_items"])
    #
    db.go_execute(sql_directory_creates["create_trigger_history_directory_items_insert"])
    db.go_execute(sql_directory_creates["create_trigger_history_directory_items_delete"])
    db.go_execute(sql_directory_creates["create_trigger_history_directory_items_update"])


def _create_quotes_environment(db: dbTolls):
    """ Создать инфраструктуру для Расценок. Таблицы, индексы и триггеры. """
    db.go_execute(sql_quotes_creates["delete_table_quotes"])
    db.go_execute(sql_quotes_creates["delete_index_quotes"])
    db.go_execute(sql_quotes_creates["delete_quotes_view_main"])
    #
    db.go_execute(sql_quotes_creates["create_table_quotes"])
    db.go_execute(sql_quotes_creates["create_index_quotes"])
    db.go_execute(sql_quotes_creates["create_view_quotes_main"])
    #
    db.go_execute(sql_quotes_creates["create_table_history_quotes"])
    db.go_execute(sql_quotes_creates["create_index_history_quotes"])
    #
    db.go_execute(sql_quotes_creates["create_trigger_history_quotes_insert"])
    db.go_execute(sql_quotes_creates["create_trigger_history_quotes_delete"])
    db.go_execute(sql_quotes_creates["create_trigger_history_quotes_update"])


def _create_catalog_environment(db: dbTolls):
    """ Создать инфраструктуру для Каталога. Таблицы, индексы и триггеры. """
    db.go_execute(sql_catalog_creates["delete_table_catalog"])
    db.go_execute(sql_catalog_creates["delete_index_catalog"])
    #
    db.go_execute(sql_catalog_creates["create_table_catalogs"])
    db.go_execute(sql_catalog_creates["create_index_catalog_code"])
    #
    db.go_execute(sql_catalog_creates["create_table_history_catalog"])
    db.go_execute(sql_catalog_creates["create_index_history_catalog"])
    #
    db.go_execute(sql_catalog_creates["create_trigger_history_catalog_insert"])
    db.go_execute(sql_catalog_creates["create_trigger_history_catalog_delete"])
    db.go_execute(sql_catalog_creates["create_trigger_history_catalog_update"])
    #
    # ---> Views -- Каталог ------------------------------------------------------------
    db.go_execute(sql_catalog_creates["create_view_catalog_main"])


def _create_quotes_chains_environment(db: dbTolls):
    """ Создать инфраструктуру для Иерархии расценок. Таблицы, индексы и триггеры. """
    db.go_execute(sql_quotes_chain_create["delete_table_quotes_chains"])
    db.go_execute(sql_quotes_chain_create["delete_index_quotes_chains"])
    db.go_execute(sql_quotes_chain_create["delete_quotes_history_quotes_chains"])
    db.go_execute(sql_quotes_chain_create["delete_index_history_quotes_chains"])


def create_tables_indexes(db_file_name: str):

    with dbTolls(db_file_name) as db:
        # --- > Справочники -- tblDirectoryItems --
        _create_directory_environment(db)
        # --- > Каталог -- tblCatalogs ---
        _create_catalog_environment(db)
        # --- > Расценки -- tblQuotes ---
        _create_quotes_environment(db)
        # --- > Иерархия расценок -- tblQuotesChains ---
        _create_quotes_chains_environment(db)


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


