import sqlite3
from icecream import ic
from config import dbTolls
from sql_queries import sql_periods_queries
from files_features import output_message_exit

def get_min_max_periods(
        db_file: str, 
        supplement_min: int, supplement_max: int,
        index_min: int, index_max: int,
    ) -> list[dict[str: any]] | None:
    """ Выбирает периоды по границам дополнений и индексов. """
    with dbTolls(db_file) as db:
        query = sql_periods_queries["get_periods_supplement_index_num"]
        query_params = (supplement_min, supplement_max, index_min, index_max)
        results = db.go_select(query, query_params)
        if results:
            return {x['id']: dict(x) for x in results}
        output_message_exit(
                f"в таблице с Периодами не найдено ни одной записи:",
                f"для границ: {query_params}")
    return None


if __name__ == "__main__":
    from data_path import set_data_location

    location = "office"  # office  # home
    local_path = set_data_location(location)
    db_file = local_path.db_file
    l = get_min_max_periods(db_file, supplement_min=67, supplement_max=72, index_min=195, index_max=209)
    ic(l)
