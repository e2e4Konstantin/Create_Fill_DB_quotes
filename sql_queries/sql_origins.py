sql_origins = {
    "select_id_name_origins": """--sql
        SELECT o.ID_tblOrigin FROM tblOrigins AS o WHERE o.name = ?;
    """,


    "delete_table_origins": """DROP TABLE IF EXISTS tblOrigins;""",
    "delete_index_origins": """DROP INDEX IF EXISTS idxOrigins;""",
    "delete_all_data_origins": """DELETE FROM tblOrigins;""",


    # --- > справочник Источников происхождения данных в таблице tblProducts
    # --- > ТСН, ПСМ, НЦКР
    "create_table_origins": """--sql
        CREATE TABLE IF NOT EXISTS tblOrigins (
                ID_tblOrigin    INTEGER PRIMARY KEY NOT NULL,
                name            TEXT NOT NULL,                              -- название
                title     	    TEXT NOT NULL,                              -- описание
                last_update     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                UNIQUE (name)
        );
    """,

    "create_index_origins": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxOrigins ON tblOrigins (name);
    """,

    "insert_origin": """
        INSERT INTO tblOrigins (name, title ) VALUES ( ?, ?);
    """,

    "select_origin_name": """--sql
        SELECT ID_tblOrigin FROM tblOrigins WHERE name = ?;
    """,

    "select_origin_id": """--sql
        SELECT * FROM tblOrigins WHERE ID_tblOrigin = ?;
    """,
}
