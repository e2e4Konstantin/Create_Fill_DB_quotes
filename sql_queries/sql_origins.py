sql_origins = {
    "delete_table_origins": """DROP TABLE IF EXISTS tblOrigins;""",
    "delete_index_origins": """DROP INDEX IF EXISTS idxOrigins;""",

    # --- > справочник Источников происхождения данных в таблице tblProducts
    # --- > ТСН, ПСМ, НЦКР
    "create_table_origins": """
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

    "select_origin_name": """
        SELECT ID_tblOrigin FROM tblOrigins WHERE name = ?;
    """,
}

