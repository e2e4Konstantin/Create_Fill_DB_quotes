sql_purchasing_storage_costs_queries = {
    "delete_table_storage_costs": """DROP TABLE IF EXISTS tblStorageCosts;""",
    "delete_index_storage_costs": """DROP INDEX IF EXISTS idxStorageCosts;""",

    "delete_table_history_storage_costs": """DROP TABLE IF EXISTS _tblHistoryStorageCosts;""",
    "delete_index_history_storage_costs": """DROP INDEX IF EXISTS idxHistoryStorageCosts;""",

    "create_table_storage_costs": """
        CREATE TABLE IF NOT EXISTS tblStorageCosts
            -- таблица для % Заготовительно-складских расходов (%ЗСР)
            (
                ID_tblStorageCosts      INTEGER PRIMARY KEY NOT NULL,
                percent_storage_costs   REAL NOT NULL DEFAULT 0.0 
                                        CHECK(percent_storage_costs >= 0 AND 
                                            percent_storage_costs <= 100), -- Процент ЗСР
                name                    TEXT NOT NULL,  -- Наименование		
                description             TEXT NOT NULL,  -- подробное описание
                last_update             INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                UNIQUE (name)
            );
        """,

    "create_index_purchasing_storage_costs": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxStorageCosts ON tblStorageCosts (name);
    """,

    # -----------------------------------------------------------------------------------------------------------------
    "create_table_history_storage_costs": """
        CREATE TABLE IF NOT EXISTS _tblHistoryStorageCosts (
        -- таблица для хранения истории %ЗСР
            _rowid                  INTEGER,
            ID_tblStorageCosts      INTEGER,
            percent_storage_costs   REAL, 
            name                    TEXT,
            description             TEXT,
            last_update             INTEGER,          
            _version                INTEGER NOT NULL,
            _updated                INTEGER NOT NULL,
            _mask                   INTEGER NOT NULL
        );
        """,

    "create_index_history_storage_costs": """
        CREATE INDEX IF NOT EXISTS idxHistoryStorageCosts ON _tblHistoryStorageCosts (_rowid);
    """,

    "create_trigger_insert_storage_costs": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryStorageCostsInsert
        AFTER INSERT ON tblStorageCosts
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCosts, 
                percent_storage_costs, name, description, last_update, 
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblStorageCosts, 
                new.percent_storage_costs, new.name, new.description, 
                new.last_update, 1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_delete_storage_costs": """
        CREATE TRIGGER tgrHistoryStorageCostsDelete
        AFTER DELETE ON tblStorageCosts
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCosts, 
                percent_storage_costs, name, description, 
                last_update, _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblStorageCosts, 
                old.percent_storage_costs, old.name, 
                old.description, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryStorageCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'), -1
            );
        END;

    """,

    "create_trigger_update_storage_cost": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryStorageCostsUpdate
        AFTER UPDATE ON tblStorageCosts
        FOR EACH ROW
        BEGIN
            -- обновление цены %ЗСР в таблице tblPropertiesMachines
            UPDATE tblPropertiesMachines 
            SET storage_costs = new.percent_storage_costs 
            WHERE FK_tblPropertiesMachine_tblStorageCosts = new.ID_tblStorageCosts; 

            
            -- History        
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCosts, 
                percent_storage_costs, name, description, last_update, 
                _version, _updated, _mask 
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblStorageCosts != new.ID_tblStorageCosts THEN new.ID_tblStorageCosts ELSE null END,
                CASE WHEN old.percent_storage_costs != new.percent_storage_costs THEN new.percent_storage_costs ELSE null END,
                CASE WHEN old.name != new.name THEN new.name ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryStorageCosts WHERE _rowid = old.rowid) + 1, 
                UNIXEPOCH('now'),
                (CASE WHEN old.ID_tblStorageCosts != new.ID_tblStorageCosts then 1 else 0 END) +
                (CASE WHEN old.percent_storage_costs != new.percent_storage_costs then 2 else 0 END) +
                (CASE WHEN old.name != new.name then 4 else 0 END) +
                (CASE WHEN old.description != new.description then 8 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 16 else 0 END)
            WHERE 
                old.ID_tblStorageCosts != new.ID_tblStorageCosts OR
                old.percent_storage_costs != new.percent_storage_costs OR
                old.name != new.name OR
                old.description != new.description OR   
                old.last_update != new.last_update;
        END;
    """,




}
