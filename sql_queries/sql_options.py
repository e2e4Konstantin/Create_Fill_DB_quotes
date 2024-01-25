
# ---> Параметры -----------------------------------------------------------------------
sql_options_queries = {
    # -- > Insert -----------------------------------------------------------------------
    "insert_option": """
        INSERT INTO tblOptions (FK_tblOptions_tblProducts, name, left_border, right_border, measurer, step, type) 
        VALUES ( ?, ?, ?, ?, ?, ?, ?);
    """,

    # -- > Delete -----------------------------------------------------------------------
    "delete_table_options": """DROP TABLE IF EXISTS tblOptions;""",
    "delete_index_options": """DROP INDEX IF EXISTS idxOptions;""",
    "delete_view_options":  """DROP VIEW IF EXISTS viewOptions;""",

    "delete_option_id_name": """
        DELETE FROM tblOptions WHERE FK_tblOptions_tblProducts = ? AND name = ?;
    """,

    "delete_option_id": """
        DELETE FROM tblOptions WHERE ID_Option = ?;
    """,

    # -- > Select -----------------------------------------------------------------------
    "select_option_id_name":   """
        SELECT * FROM tblOptions WHERE FK_tblOptions_tblProducts = ? AND name = ?;
    """,

    # ---> Create --------------------------------------------------------------------
    # ---> Период, к которому относится параметр, будет взят из периода расценки/продукта.

    "create_table_options": """
        CREATE TABLE IF NOT EXISTS tblOptions
            (
                ID_Option                   INTEGER PRIMARY KEY NOT NULL,
                FK_tblOptions_tblProducts   INTEGER NOT NULL,   -- id владельца 
                name                        TEXT NOT NULL,      -- название параметра
                left_border	                REAL,               -- от
                right_border	            REAL,               -- до
                measurer                    TEXT, 	            -- единица измерения
                step	                    TEXT,               -- шаг
                type                        INTEGER DEFAULT 0,  -- тип
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')), -- время последнего обновления
                FOREIGN KEY (FK_tblOptions_tblProducts) REFERENCES tblProducts (ID_tblProduct),
                UNIQUE (FK_tblOptions_tblProducts, name)
            );
    """,

    "create_index_options": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxOptions ON tblOptions (FK_tblOptions_tblProducts, name);
    """,

    # ---> Представления --------------------------------------------------------------------

    "create_view_options": """
        CREATE VIEW viewOptions AS
            SELECT
                r.code AS 'шифр',
                r.period AS 'период', 
                o.name AS 'параметр',
                o.left_border AS 'от',
                o.right_border AS 'до',
                o.measurer AS 'ед.изм',
                o.step AS 'шаг',
                o.type AS 'тип'
            FROM tblOptions o
            LEFT JOIN tblProducts AS r ON r.ID_tblProduct = o.FK_tblOptions_tblProducts
            ORDER BY r.code;
    """,


# --- > История изменения таблицы Параметров -----------------------------------
    "create_table_history_options": """
        CREATE TABLE IF NOT EXISTS _tblHistoryOptions (
            _rowid          INTEGER,
            ID_Option    INTEGER,
            FK_tblOptions_tblProducts INTEGER, 
            name            TEXT,
            left_border     REAL, 
            right_border	REAL,              
            measurer        TEXT, 	           
            step	        TEXT,              
            type            INTEGER DEFAULT 0, 
            last_update     INTEGER,          
            _version        INTEGER NOT NULL,
            _updated        INTEGER NOT NULL,
            _mask           INTEGER NOT NULL
        );
        """,

    "create_index_history_options": """
        CREATE INDEX IF NOT EXISTS idxHistoryOptions ON _tblHistoryOptions (_rowid);
    """,

    "create_trigger_history_options_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryOptionsInsert
        AFTER INSERT ON tblOptions
        BEGIN
            INSERT INTO _tblHistoryOptions (
                _rowid, ID_Option, FK_tblOptions_tblProducts, 
                name, left_border, right_border, measurer, step, type, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_Option, new.FK_tblOptions_tblProducts, 
                new.name, new.left_border, new.right_border, 
                new.measurer, new.step, new.type, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_options_delete": """
        CREATE TRIGGER tgrHistoryOptionsDelete
        AFTER DELETE ON tblOptions
        BEGIN
            INSERT INTO _tblHistoryOptions (
                _rowid, ID_Option, FK_tblOptions_tblProducts, 
                name, left_border, right_border, measurer, step, type, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                old.rowid, old.ID_Option, old.FK_tblOptions_tblProducts, 
                old.name, old.left_border, old.right_border,
                old.measurer, old.step, old.type, old.last_update,
                 
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryOptions WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,
}
