
from icecream import ic


from config import dbTolls
from sql_queries import sql_periods_queries, sql_origins, sql_items_queries
from files_features import output_message_exit


def get_periods_range(
    db_file: str, origin_name: str, period_item_type: str,
    supplement_min: int, supplement_max: int
) -> list[dict[str:any]] | None:
    """
    Для каталога 'origin_name' и типа периода 'period_type'
    выбирает периоды для диапазону номеров min -> max.
    Возвращает список отсортированный по возрастанию номеров дополнений.
    """
    with dbTolls(db_file) as db:
        origin_id = db.get_row_id(sql_origins["select_id_name_origins"], (origin_name, ))
        items_id = db.get_row_id(
            sql_items_queries["select_item_id_team_name"],
            ("periods_category", period_item_type),
        )
        query_params = (origin_id, items_id, supplement_min, supplement_max)
        results = db.go_select(
            sql_periods_queries["get_periods_supplement_num"], query_params)
        if results:
            periods = [dict(x) for x in results]
            periods.sort(reverse=False, key=lambda x: x['supplement'])
            return periods
        output_message_exit(
                "в таблице с Периодами не найдено ни одной записи:",
                f"для границ: {query_params}")
    return None


if __name__ == "__main__":
    from config import LocalData

    local = "office"  # office  # home
    db_file = LocalData(local).db_file

    result = get_periods_range(
        db_file, origin_name="ТСН", period_item_type="supplement",
        supplement_min=67, supplement_max=72,
    )
    ic(result)
