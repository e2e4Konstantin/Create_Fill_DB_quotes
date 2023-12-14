
sql_bases_creates = {

    # --- > Базовая таблица для хранения Расценок, Материалов, Машин и Оборудования ----
    "create_table_bases": """
        CREATE TABLE IF NOT EXISTS tblBases
            (
                ID_tblBase   INTEGER PRIMARY KEY NOT NULL,
                FK_tblBases_tblCatalogs INTEGER NOT NULL,            -- id элемента каталога
                type INTEGER NOT NULL CHECK("type" in (1, 2, 3, 4)), -- материал/машина/расценка/оборудование
                -- 
                period      INTEGER NOT NULL,   -- период
                code	 	TEXT NOT NULL,		-- шифр					
                description TEXT NOT NULL,      -- описание
                measurer    TEXT,               -- единица измерения
                full_code   TEXT,               -- полный шифр    
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                FOREIGN KEY (FK_tblBases_tblCatalogs) REFERENCES tblCatalogs (ID_tblCatalog),
                UNIQUE (code)
            );
        """,

    "create_index_bases": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxBasesCode ON tblBases (code, period);
    """,

    "delete_table_bases": """DROP TABLE IF EXISTS tblBases;""",
    "delete_index_bases": """DROP INDEX IF EXISTS idxBasesCode;""",
    "delete_view_bases": """DROP VIEW IF EXISTS viewBases; """,

    "delete_table_bases_history": """DROP TABLE IF EXISTS _tblHistoryBases;""",
    "delete_index_bases_history": """DROP INDEX IF EXISTS idxHistoryBases;""",


    # --- > История базовой таблицы -----------------------------------------------------
    "create_table_history_bases": """
        CREATE TABLE IF NOT EXISTS _tblHistoryBases (
            _rowid        INTEGER,
            
            ID_tblBase   INTEGER,
            FK_tblBases_tblCatalogs INTEGER,
            type          INTEGER,
            period        INTEGER,
            code	 	  TEXT,
            description	  TEXT,
            measurer      TEXT,
            full_code     TEXT,
                                 
            last_update   INTEGER,          
            
            _version      INTEGER NOT NULL,
            _updated      INTEGER NOT NULL,
            _mask         INTEGER NOT NULL
        );
        """,

    "create_index_history_bases": """
        CREATE INDEX IF NOT EXISTS idxHistoryBases ON _tblHistoryBases (_rowid);
    """,
    # --- > Триггеры базовой таблицы -----------------------------------------------------

    "create_trigger_history_bases_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryBasesInsert
        AFTER INSERT ON tblBases
        BEGIN
            INSERT INTO _tblHistoryBases (
                _rowid, ID_tblBase, FK_tblBases_tblCatalogs, 
                period, code, description, measurer, full_code, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_tblBase, new.FK_tblBases_tblCatalogs, 
                new.period, new.code, new.description, new.measurer, new.full_code, last_update, 
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_bases_delete": """
        CREATE TRIGGER tgrHistoryBasesDelete
        AFTER DELETE ON tblBases
        BEGIN
            INSERT INTO _tblHistoryBases (
                _rowid, ID_tblBase, FK_tblBases_tblCatalogs, 
                period, code, description, measurer, full_code, last_update, 
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblBase, old.FK_tblBases_tblCatalogs, 
                old.period, old.code, old.description, old.measurer, old.full_code, old.last_update, 
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryBases WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_bases_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryBasesUpdate
        AFTER UPDATE ON tblBases
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryBases (
                _rowid, ID_tblBase, FK_tblBases_tblCatalogs, 
                period, code, description, measurer, full_code, last_update, 
                _version, _updated, _mask
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblBase != new.ID_tblBase THEN new.ID_tblBase ELSE null END,
                CASE WHEN old.FK_tblBases_tblCatalogs != new.FK_tblBases_tblCatalogs THEN new.FK_tblBases_tblCatalogs ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.measurer != new.measurer THEN new.measurer ELSE null END,
                CASE WHEN old.full_code != new.full_code THEN new.full_code ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                
                (SELECT MAX(_version) FROM _tblHistoryBases WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                
                (CASE WHEN old.ID_tblBase != new.ID_tblBase then 1 else 0 END) +
                (CASE WHEN old.FK_tblBases_tblCatalogs != new.FK_tblBases_tblCatalogs then 2 else 0 END) +
                (CASE WHEN old.period != new.period then 4 else 0 END) +
                (CASE WHEN old.code != new.code then 8 else 0 END) +
                (CASE WHEN old.description != new.description then 16 else 0 END) +
                (CASE WHEN old.measurer != new.measurer then 32 else 0 END) +
                (CASE WHEN old.full_code != new.full_code then 64 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 128 else 0 END)
            WHERE 
                old.ID_tblBase != new.ID_tblBase OR
                old.FK_tblBases_tblCatalogs != new.FK_tblBases_tblCatalogs OR
                old.period != new.period OR
                old.code != new.code OR
                old.description != new.description OR
                old.measurer != new.measurer OR
                old.full_code != new.full_code OR
                old.last_update != new.last_update;
        END;
    """,

    "create_view_bases": """
        CREATE VIEW viewBases AS
            SELECT 
                i.name AS parent,
                c.code AS parent_code,
                b.code AS code,
                b.description AS title,
                b.measurer AS measurer
            FROM tblBases b 
            LEFT JOIN tblCatalogs AS c ON c.ID_tblCatalog = b.FK_tblBases_tblCatalogs
            LEFT JOIN tblDirectoryItems AS i ON i.ID_tblDirectoryItem = c.FK_tblCatalogs_tblDirectoryItems
            ORDER BY quotes_code;
    """,



}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
