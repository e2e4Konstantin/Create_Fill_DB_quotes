sql_catalog = {

    # --- > Таблица для хранения различных иерархических справочников значений ---------------------
    #
    # tblDirectoryItems
    # 'глава',   'сборник',    'отдел',   'раздел',     'таблица', 'расценка'
    # 'chapter', 'collection', 'section', 'subsection', 'table',   'quote']
    # ID	ID_Parent	Code        Name
    #______________________________________________________________
    # 1	    null	Directory	    Справочник значений
    # 2	    1	    CatalogItems   	Значения объектов каталога
    # 3	    2	    Chapter     	Глава
    # 4	    3	    Collection	    Сборник
    # 5	    4	    Section	        Отдел
    # 6	    5	    Subsection	    Раздел
    # 7	    6	    Table   	    Таблица
    # 8	    7	    Quote	        Расценка

    "create_table_directory_items": """
        CREATE TABLE IF NOT EXISTS tblDirectoryItems (
                ID_tblDirectoryItem INTEGER PRIMARY KEY NOT NULL,
                ID_parent           INTEGER REFERENCES tblDirectoryItems (ID_tblDirectoryItem) DEFAULT NULL,
                code                TEXT NOT NULL,
                name       	        TEXT NOT NULL,
                last_update         INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                UNIQUE (code));
    """,

    "create_index_director_items": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxDirectoryCode ON tblDirectoryItems (code);
    """,

    "insert_root_item_directory": """
        INSERT INTO tblDirectoryItems (ID_parent, code, name) VALUES (?, ?, ?);
    """,

    "insert_item_directory": """
        INSERT INTO tblDirectoryItems (ID_parent, code, name)
        VALUES (
        (SELECT ID_tblDirectoryItem FROM tblDirectoryItems WHERE code= ?), ?, ?
        );
    """,

    "delete_table_directory": """DROP TABLE IF EXISTS tblDirectoryItems;""",
    "delete_index_directory": """DROP INDEX IF EXISTS idxDirectoryCodes;""",

    # --- > таблица для хранения истории изменений Справочников ---------------------------
    # _mask битовая маска указывает в каком поле были изменения. Вычисляется как сложение вместе следующих значений:
    # 1:  ID_tblDirectoryItem
    # 2:  ID_parent
    # 4:  code
    # 8:  name
    # 16: last_update
    # _mask равная -1 показывает что запись была удалена.
    "create_table_history_directory_items": """
        CREATE TABLE IF NOT EXISTS _tblHistoryDirectoryItems (
            _rowid              INTEGER,
            ID_tblDirectoryItem INTEGER,
            ID_parent           INTEGER,
            code                TEXT,
            name       	        TEXT,
            last_update         INTEGER,          
            _version            INTEGER NOT NULL,
            _updated            INTEGER NOT NULL,
            _mask               INTEGER NOT NULL
        );
        """,


    "create_index_history_directory_items": """
        CREATE INDEX IF NOT EXISTS idxHistoryDirectoryItemsRowId ON _tblHistoryDirectoryItems (_rowid);
    """,

    "create_trigger_history_directory_items_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryDirectoryItemsInsert
        AFTER INSERT ON tblDirectoryItems
        BEGIN
            INSERT INTO _tblHistoryDirectoryItems (
                _rowid, ID_tblDirectoryItem, ID_parent, code, name, last_update, 
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblDirectoryItem, new.ID_parent, new.code, new.name, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_directory_items_delete": """
        CREATE TRIGGER tgrHistoryDirectoryItemsDelete
        AFTER DELETE ON tblDirectoryItems
        BEGIN
            INSERT INTO _tblHistoryDirectoryItems (
                _rowid, ID_tblDirectoryItem, ID_parent, code, name, last_update, 
                _version, 
                _updated, _mask
            )
            VALUES (
                old.rowid, 
                old.ID_tblDirectoryItem, old.ID_parent, old.code, old.name, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryDirectoryItems WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_directory_items_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryDirectoryItemsUpdate
        AFTER UPDATE ON tblDirectoryItems
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryDirectoryItems (
                _rowid, ID_tblDirectoryItem, ID_parent, code, name, last_update, 
                _version, _updated, _mask
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblDirectoryItem != new.ID_tblDirectoryItem THEN new.ID_tblDirectoryItem ELSE null END,
                CASE WHEN old.ID_parent != new.ID_parent THEN new.ID_parent ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.name != new.name THEN new.name ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryDirectoryItems WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                (CASE WHEN old.ID_tblDirectoryItem != new.ID_tblDirectoryItem then 1 else 0 END) +
                (CASE WHEN old.ID_parent != new.ID_parent then 2 else 0 END) +
                (CASE WHEN old.code != new.code then 8 else 0 END) +
                (CASE WHEN old.name != new.name then 16 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 32 else 0 END)
            WHERE 
                old.ID_tblDirectoryItem != new.ID_tblDirectoryItem OR
                old.ID_parent != new.ID_parent OR
                old.code != new.code OR
                old.name != new.name OR   
                old.last_update != new.last_update;
        END;
    """,

    "delete_table_history_directory": """DROP TABLE IF EXISTS _tblHistoryDirectoryItems;""",
    "delete_index_history_directory": """DROP INDEX IF EXISTS idxHistoryDirectoryItemsRowId;""",

    "delete_trigger_history_directory_items_insert": """DROP TRIGGER IF EXISTS tgrHistoryDirectoryItemsInsert;""",
    "delete_trigger_history_directory_items_delete": """DROP TRIGGER IF EXISTS tgrHistoryDirectoryItemsDelete;""",
    "delete_trigger_history_directory_items_update": """DROP TRIGGER IF EXISTS tgrHistoryDirectoryItemsUpdate;""",

}

