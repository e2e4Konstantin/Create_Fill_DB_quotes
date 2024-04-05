from config import dbTolls
from sql_queries import sql_storage_costs_queries


def create_storage_costs_environment(db: dbTolls):
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


if __name__ == "__main__":
    from config import LocalData

    location = "office"  # office  # home

    with dbTolls(LocalData(location).db_file) as db:
        create_storage_costs_environment(db)

