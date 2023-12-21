sql_raw_queries = {
    # --- > Имена таблиц --------------------------------------------------------------------------
    "table_name_raw_data":       """tblRawData""",
    # --- > Удаление таблиц -----------------------------------------------------------------------
    "delete_table_raw_data":     """DROP TABLE IF EXISTS tblRawData;""",

    # --- > Получение данных ----------------------------------------------------------------------
    "select_rwd_all": """SELECT * FROM tblRawData;""",
    "select_rwd_code_regexp": """
        SELECT * FROM tblRawData WHERE PRESSMARK REGEXP ?;
    """,

    # Материалы Глава 1
    "select_rwd_materials_catalog": """
        SELECT ID, PARENT, CMT, TITLE, "Ед.Изм.", "Брутто", PERIOD 
        FROM tblRawData 
        WHERE "Ед.Изм." IS NULL OR "Брутто" IS NULL;
    """,

    "select_rwd_materials_items_re_catalog": """
        SELECT ID, PARENT, CMT, TITLE, PERIOD 
        FROM tblRawData 
        WHERE ("Ед.Изм." IS NULL OR "Брутто" IS NULL) AND CMT REGEXP ?;        
    """,

    "select_rwd_material_cmt_id": """
        SELECT CMT FROM tblRawData WHERE id = ?;
    """,
}
