import sqlite3
from icecream import ic
from config import dbTolls
from sql_queries import sql_periods_queries, sql_origins, sql_items_queries
from files_features import output_message_exit


def get_supplement_periods(db_file: str, sup_min: int, sup_max: int) -> list[dict[str: any]] | None:
    """ Для каталога 'ТСН' выбирает периоды дополнений по диапазону номеров min -> max. """
    with dbTolls(db_file) as db:
        origin_id = db.get_row_id(sql_origins["select_id_name_origins"], ('ТСН', ))
        items_category_id = db.get_row_id(
            sql_items_queries["select_item_id_team_name"],
            ('periods_category', 'дополнение'))
        query_params = (origin_id, items_category_id, sup_min, sup_max)
        results = db.go_select(
            sql_periods_queries["get_periods_supplement_num"], query_params)
        if results:
            return [dict(x) for x in results]
        output_message_exit(
                "в таблице с Периодами не найдено ни одной записи:",
                f"для границ: {query_params}")
    return None


if __name__ == "__main__":
    from config import LocalData, get_data_location

    location = "office"  # office  # home
    paths: LocalData = get_data_location(location)
    db_file = paths.db_file

    l = get_supplement_periods(db_file, sup_min=67, sup_max=72)
    ic(l)
