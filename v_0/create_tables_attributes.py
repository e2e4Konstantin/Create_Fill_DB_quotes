from config import dbTolls
from sql_queries import sql_attributes_creates


def create_tables_idx_attributes(db_file_name: str):
    with dbTolls(db_file_name) as db:
        # --- > Удаляем таблицы, индексы и представления --------------------------------
        db.go_execute(sql_attributes_creates["delete_table_attributes"])
        db.go_execute(sql_attributes_creates["delete_index_attributes"])
        db.go_execute(sql_attributes_creates["delete_view_attributes"])

        # --- > Атрибуты  ---------------------------------------------------------------
        db.go_execute(sql_attributes_creates["create_table_attributes"])
        db.go_execute(sql_attributes_creates["create_index_attributes"])

        # ---> Views Атрибуты -----------------------------------------------------------
        db.go_execute(sql_attributes_creates["create_view_attributes"])


if __name__ == '__main__':
    import os
    from icecream import ic

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")
    ic(db_name)

    create_tables_idx_attributes(db_name)
