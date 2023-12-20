sql_raw_queries = {
    # --- > Имена таблиц --------------------------------------------------------------------------
    "table_name_raw_data":       """tblRawData""",
    # --- > Удаление таблиц -----------------------------------------------------------------------
    "delete_table_raw_data":     """DROP TABLE IF EXISTS tblRawData;""",

    # --- > Получение данных ----------------------------------------------------------------------
    "select_rwd_all": """SELECT * FROM tblRawData;""",
    "select_rwd_code_regexp": """SELECT * FROM tblRawData WHERE PRESSMARK REGEXP ?;""",

    # Машины Глава 1
    "select_rwd_machines_catalog": """ SELECT r.* FROM tblRawData r WHERE r."Ед.Изм." IS NULL OR r.Брутто IS NULL;"""
}
