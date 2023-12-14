
sql_items_queries = {
    "select_items":         """SELECT * FROM tblItems;""",
    "select_items_id_code": """SELECT ID_tblItem FROM tblItems WHERE code IS ?;""",

}

sql_items_creates = {
    "delete_table_items": """DROP TABLE IF EXISTS tblItems;""",
    "delete_index_items": """DROP INDEX IF EXISTS idxItems;""",

    "delete_table_items_history": """DROP TABLE IF EXISTS _tblHistoryItems;""",
    "delete_index_items_history": """DROP INDEX IF EXISTS idxHistoryItems;""",

    # --- > Таблица для хранения справочников ---------------------
    #  в справочнике 'clip' хранятся категории элементов каталога
    #  в справочнике 'unit' хранятся категории для базовой таблицы Расценок, Материалов, Машин и Оборудования
    #  поле team определяет название справочника
    "create_table_items": """
        CREATE TABLE IF NOT EXISTS tblItems (
                ID_tblItem INTEGER PRIMARY KEY NOT NULL,
                team               TEXT NOT NULL, 
                code                TEXT NOT NULL,
                name       	        TEXT NOT NULL,
                last_update         INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                UNIQUE (team, code)
        );
    """,

    "create_index_items": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxItems ON tblItems (team, code);
    """,

    "insert_item": """
        INSERT INTO tblItems (team, code, name) VALUES ( ?, ?, ?);
    """,

    # --- > таблица для хранения истории изменений Справочников ---------------------------
    # _mask битовая маска указывает в каком поле были изменения. Вычисляется как сложение вместе следующих значений:
    # 1:  ID_tblDirectoryItem
    # 2:  team
    # 4:  code
    # 8:  name
    # 16: last_update
    # _mask равная -1 показывает что запись была удалена.
    "create_table_history_items": """
        CREATE TABLE IF NOT EXISTS _tblHistoryItems (
            _rowid      INTEGER,
            ID_tblItem  INTEGER,
            team       TEXT,
            code        TEXT,
            name       	TEXT,
            last_update INTEGER,          
            _version    INTEGER NOT NULL,
            _updated    INTEGER NOT NULL,
            _mask       INTEGER NOT NULL
        );
        """,

    "create_index_history_items": """
        CREATE INDEX IF NOT EXISTS idxHistoryItems ON _tblHistoryItems (_rowid);
    """,

    "create_trigger_history_items_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryItemsInsert
        AFTER INSERT ON tblItems
        BEGIN
            INSERT INTO _tblHistoryItems (
                _rowid, ID_tblItem, team, code, name, last_update, 
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblItem, new.team, new.code, new.name, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_items_delete": """
        CREATE TRIGGER tgrHistoryItemsDelete
        AFTER DELETE ON tblItems
        BEGIN
            INSERT INTO _tblHistoryItems (
                _rowid, ID_tblItem, team, code, name, last_update, 
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, 
                old.ID_tblItem, old.team, old.code, old.name, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryItems WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_items_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryItemsUpdate
        AFTER UPDATE ON tblItems
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryItems (
                _rowid, ID_tblItem, team, code, name, last_update, 
                _version, _updated, _mask
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblItem != new.ID_tblItem THEN new.ID_tblItem ELSE null END,
                CASE WHEN old.team != new.team THEN new.team ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.name != new.name THEN new.name ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryItems WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                (CASE WHEN old.ID_tblItem != new.ID_tblItem then 1 else 0 END) +
                (CASE WHEN old.team != new.team then 2 else 0 END) +
                (CASE WHEN old.code != new.code then 4 else 0 END) +
                (CASE WHEN old.name != new.name then 8 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 16 else 0 END)
            WHERE 
                old.ID_tblItem != new.ID_tblItem OR
                old.team != new.team OR
                old.code != new.code OR
                old.name != new.name OR   
                old.last_update != new.last_update;
        END;
    """,

}

