


#
# def _get_item_pattern(db: dbTolls, directory_name: str, item_name: str) -> str | None:
#     items_cursor = db.go_execute(sql_items_queries["select_items_all_team_name"], (directory_name, item_name))
#     item = items_cursor.fetchone() if items_cursor else None
#     return item['re_pattern'] if item else None