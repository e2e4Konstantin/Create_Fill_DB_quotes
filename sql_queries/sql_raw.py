sql_raw_queries = {
    # --- > Имена таблиц --------------------------------------------------------------------------
    "table_name_raw_data": """tblRawData""",
    "create_index_raw_data": """CREATE INDEX IF NOT EXISTS idxTmpMaterials ON tblRawData (ID, PARENT, CMT);""",
    "create_index_raw_code": """--sql
        CREATE INDEX IF NOT EXISTS idxRawGwpCodeResources ON tblRawData (gwp_pressmark, pressmark);
    """,
    # индекс для переноса Ресурсов глава 1 и 2
    "create_index_raw_resources": """--sql
        CREATE INDEX IF NOT EXISTS idxRawCodeGrResources ON tblRawData (pressmark, id_group_resource);
    """,
    "add_index_number_column": """--sql
        ALTER TABLE tblRawData ADD COLUMN index_number INTEGER;
    """,
    "add_digit_code_column": """--sql
        ALTER TABLE tblRawData ADD digit_code INTEGER;
    """,
    # --- > Удаление ------------------------------------------------------------------------------
    "delete_table_raw_data": """DROP TABLE IF EXISTS tblRawData;""",
    "delete_index_raw_data": """DROP INDEX IF EXISTS idxTmpMaterial;""",
    "delete_index_raw_resources": """DROP INDEX IF EXISTS idxRawCodeGrResources;""",
    "delete_index_raw_catalog_resources": """DROP INDEX IF EXISTS idxRawGwpCodeResources;""",
    "delete_raw_data_old_periods": """DELETE FROM tblRawData WHERE DATE(date_start) <= '2020-01-01';""",
    # --- > Получение данных ----------------------------------------------------------------------
    "update_digit_code": """--sql
        UPDATE tblRawData SET digit_code = :digit_code where rowid = :id;
    """,
    "round_supplier_price_for_monitoring_materials": """--sql
        UPDATE tblRawData SET supplier_price = ROUND(supplier_price, ?);
    """,
    #
    "round_price_for_monitoring_transport_costs": """--sql
        UPDATE tblRawData SET current_price = ROUND(current_price, ?);
    """,
    #
    "update_index_number": """--sql
        UPDATE tblRawData
        SET index_number = CAST(substr(title_period,1,INSTR(title_period, ' ')) AS INTEGER);
    """,
    "select_rwd_all": """--sql
        SELECT rowid, * FROM tblRawData;
    """,
    "select_monitoring_materials_rwd_all": """--sql
        SELECT rowid, * FROM tblRawData order by digit_code;
    """,
    "select_rwd_all_sorted_by_index_number": """--sql
        SELECT * FROM tblRawData ORDER BY index_number ASC;
    """,
    "select_rwd_for_normative_period_id": """--sql
        SELECT * FROM tblRawData WHERE period_id = ?;
    """,
    "select_rwd_for_normative_period_id_order_pressmark": """--sql
        SELECT * FROM tblRawData WHERE period_id = ? ORDER BY pressmark;
    """,
    "select_rwd_code_regexp": """--sql
        SELECT
            (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent AS INT) ) AS [parent_pressmark],
            m.*
        FROM tblRawData AS m WHERE m.pressmark REGEXP ?;
    """,
    # "select_rwd_code_regexp": """--sql
    #     SELECT * FROM tblRawData WHERE PRESSMARK REGEXP ?;
    # """,
    "select_raw_with_parent_pressmark": """--sql
        SELECT
            (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent AS INT) ) AS [parent_pressmark],
            m.*
        FROM tblRawData AS m
        ORDER BY m.pressmark_sort
    """,
    "select_raw_resource_by_code": """--sql
        /* формирует таблицу подставляет шифры родителей по id, выбирает запись с указанным шифром */
        SELECT
            (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*
        FROM tblRawData AS m
        WHERE m.pressmark REGEXP ?;
    """,
    "select_raw_items_by_chapter": """--sql
        /* два параметра 1: '(^\s*1\..*)|(^\s*1\s*$)' 2: '^\s*(\d+)\s*$' */
        SELECT
        (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*, m.*
        FROM (SELECT f.* FROM tblRawData f WHERE f.pressmark REGEXP ?) AS m
        WHERE m.pressmark REGEXP ?;
    """,
    # Материалы Глава 1
    "select_raw_like_pressmark": """--sql
        SELECT * FROM tblRawData r WHERE r.pressmark LIKE ?;
    """,
    "select_raw_chapter_1": """--sql
        SELECT * FROM tblRawData WHERE pressmark REGEXP '^\s*[(1\.)|1]';
    """,
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
    # --- > ресурсы ПСМ , главы 71, 72, 73
    "select_rwd_resources": """
        SELECT
            "Шифр новый действующий" AS CODE,
            "Уточненное наименование по данным мониторинга" as TITLE,
            "Ед. изм." AS MEASURE,
            PERIOD
        FROM tblRawData
        WHERE CODE REGEXP ?;
    """,
    # PRAGMA table_info(tblRawData);
    "test_pragma": """
        SELECT name FROM pragma_table_info('tblRawData') where rowid = 4
    """,
}
