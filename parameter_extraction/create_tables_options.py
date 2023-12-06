from config import dbTolls
from sql_queries import sql_options_queries


def create_tables_idx_options(db_file_name: str):
    with dbTolls(db_file_name) as db:
        # --- > Удаляем таблицы, индексы и представления --------------------------------
        db.go_execute(sql_options_queries["delete_table_options"])
        db.go_execute(sql_options_queries["delete_index_options"])
        db.go_execute(sql_options_queries["delete_view_options"])

        # --- > Параметры  ---------------------------------------------------------------
        db.go_execute(sql_options_queries["create_table_options"])
        db.go_execute(sql_options_queries["create_index_options"])

        # ---> Views Атрибуты -----------------------------------------------------------
        db.go_execute(sql_options_queries["create_view_options"])


if __name__ == '__main__':
    import os
    from icecream import ic

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")
    ic(db_name)

    create_tables_idx_options(db_name)
