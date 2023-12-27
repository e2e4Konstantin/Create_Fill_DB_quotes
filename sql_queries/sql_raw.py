sql_raw_queries = {
    # --- > Имена таблиц --------------------------------------------------------------------------
    "table_name_raw_data":       """tblRawData""",

    "create_index_raw_data":     """CREATE INDEX IF NOT EXISTS idxTmpMaterials ON tblRawData (ID, PARENT, CMT);""",

    # --- > Удаление таблиц -----------------------------------------------------------------------
    "delete_table_raw_data":     """DROP TABLE IF EXISTS tblRawData;""",
    "delete_index_raw_data":     """DROP INDEX IF EXISTS idxTmpMaterial;""",

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

    "select_rwd_cmt_id": """
        SELECT CMT FROM tblRawData WHERE id = ?;
    """,

    "select_rwd_materials": """
        SELECT ID, PARENT, CMT, TITLE, "Ед.Изм.", "Брутто", PERIOD 
        FROM tblRawData 
        WHERE ("Ед.Изм." IS NOT NULL OR "Брутто" IS NOT NULL);
    """,
    # --- > машины, глава 2
    "select_rwd_machines": """
        SELECT ID, PARENT, CMT, TITLE, "Ед.Изм.", "Брутто", PERIOD 
        FROM tblRawData 
        WHERE ("Ед.Изм." IS NOT NULL OR "Брутто" IS NOT NULL);
    """,

    # --- > оборудование, глава 13
    "select_rwd_equipments": """
        SELECT ID, PARENT, CMT, TITLE, "Ед.Изм.", "Брутто", PERIOD 
        FROM tblRawData 
        WHERE ("Ед.Изм." IS NOT NULL OR "Брутто" IS NOT NULL);
    """,


}
