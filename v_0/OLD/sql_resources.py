sql_resources_create = {
    # Шифр	Наименование	Ед.изм	ОКП	ОКПД2	Базисная цена
    # --- > Ресурсы ---------------------------------------------------------------------
    "create_table_resources": """
        CREATE TABLE IF NOT EXISTS tblResources
            (
                ID_tblResource INTEGER PRIMARY KEY NOT NULL,
                --
                FK_tblResources_tblCatalogs INTEGER NOT NULL, -- id материнского элемента каталога
                --
                period        INTEGER NOT NULL, -- период
                code	 	  TEXT NOT NULL,	-- шифр					
                description	  TEXT NOT NULL,    -- описание
                measurer      TEXT,             -- единица измерения
                okp	          TEXT,             -- ОКП
                okpd2         TEXT,	            -- ОКПД2
                base_price    REAL,             -- прямые затраты
                --
                netto            REAL,	        -- нетто
                brutto           REAL,          -- брутто
                comment          TEXT,          -- уточнение
                salary	         REAL,          -- фонд оплаты труда
                electricity_cost REAL,          -- электроэнергия
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                FOREIGN KEY (FK_tblResources_tblCatalogs) REFERENCES tblCatalogs (ID_tblCatalog),
                UNIQUE (code, okp, okpd2)
            );
        """,

    "create_index_resources": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxResources ON tblResources (code, okp, okpd2);
    """,

    # --- > История хранения Ресурсов ---------------------------------------------------

    "create_table_history_resource": """
        CREATE TABLE IF NOT EXISTS _tblHistoryResources (
            _rowid                      INTEGER NOT NULL,

            ID_tblResource              INTEGER,
            FK_tblResources_tblCatalogs INTEGER,
            period                      INTEGER,
            code	 	                TEXT,					
            description	                TEXT,
            measurer                    TEXT, 
            okp	                        TEXT,
            okpd2                       TEXT,
            base_price                  REAL,
            --
            netto                       REAL,	       
            brutto                      REAL,     
            comment                     TEXT,     
            salary	                    REAL,     
            electricity_cost            REAL,     
            --          
            last_update                 INTEGER,
            
            _version            INTEGER NOT NULL,
            _updated            INTEGER NOT NULL,
            _mask               INTEGER NOT NULL
        );
        """,

    "create_index_history_resources": """
        CREATE INDEX IF NOT EXISTS idxHistoryResources ON _tblHistoryResources (_rowid);
    """,

    # --- > Триггеры для Ресурсов -------------------------------------------------------

    "create_trigger_history_resources_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryResourcesInsert
        AFTER INSERT ON tblResources
        BEGIN
            INSERT INTO _tblHistoryResources (
                _rowid, 
                ID_tblResource, FK_tblResources_tblCatalogs, period, 
                code, description, measurer, okp, okpd2, base_price, 
                netto, brutto, comment, salary, electricity_cost,
                last_update,
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, 
                new.ID_tblResource, new.FK_tblResources_tblCatalogs, new.period, 
                new.code, new.description, new.measurer, new.okp, new.okpd2, new.base_price, 
                new.netto, new.brutto, new.comment, new.salary, new.electricity_cost,
                new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_resources_delete": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryResourcesDelete
        AFTER DELETE ON tblResources
        BEGIN
            INSERT INTO _tblHistoryResources (
                _rowid, 
                ID_tblResource, FK_tblResources_tblCatalogs, period, 
                code, description, measurer, okp, okpd2, base_price, 
                netto, brutto, comment, salary, electricity_cost,
                last_update,
                _version, 
                _updated, _mask 
            )
            VALUES (
                old.rowid, 
                old.ID_tblResource, old.FK_tblResources_tblCatalogs, old.period, 
                old.code, old.description, old.measurer, old.okp, old.okpd2, old.base_price, 
                old.netto, old.brutto, old.comment, old.salary, old.electricity_cost,  
                old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryResources WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_resources_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryResourcesUpdate
        AFTER UPDATE ON tblResources
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryResources (
                _rowid, 
                ID_tblResource, FK_tblResources_tblCatalogs, period, 
                code, description, measurer, okp, okpd2, base_price,
                netto, brutto, comment, salary, electricity_cost,
                last_update,
                _version, 
                _updated, 
                _mask 
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblResource != new.ID_tblResource THEN new.ID_tblResource ELSE null END,
                CASE WHEN old.FK_tblResources_tblCatalogs != new.FK_tblResources_tblCatalogs THEN new.FK_tblResources_tblCatalogs ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.measurer != new.measurer THEN new.measurer ELSE null END,
                CASE WHEN old.okp != new.okp THEN new.okp ELSE null END,
                CASE WHEN old.okpd2 != new.okpd2 THEN new.okpd2 ELSE null END,
                CASE WHEN old.base_price != new.base_price THEN new.base_price ELSE null END,
                
                CASE WHEN old.netto != new.netto THEN new.netto ELSE null END,
                CASE WHEN old.brutto != new.brutto THEN new.brutto ELSE null END,
                CASE WHEN old.comment != new.comment THEN new.comment ELSE null END,
                CASE WHEN old.salary != new.salary THEN new.salary ELSE null END,
                CASE WHEN old.electricity_cost != new.electricity_cost THEN new.electricity_cost ELSE null END,
                
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryResources WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), 

                (CASE WHEN old.ID_tblResource != new.ID_tblResource then 1 else 0 END) +
                (CASE WHEN old.FK_tblResources_tblCatalogs != new.FK_tblResources_tblCatalogs then 2 else 0 END) +
                (CASE WHEN old.period != new.period then 4 else 0 END) +
                (CASE WHEN old.code != new.code then 8 else 0 END) +
                (CASE WHEN old.description != new.description then 16 else 0 END) +
                (CASE WHEN old.measurer != new.measurer then 32 else 0 END) +
                (CASE WHEN old.okp != new.okp then 64 else 0 END) +
                (CASE WHEN old.okpd2 != new.okpd2 then 128 else 0 END) +
                (CASE WHEN old.base_price != new.base_price then 256 else 0 END) +
                
                (CASE WHEN old.netto != new.netto then 512 else 0 END) +
                (CASE WHEN old.brutto != new.brutto then 1024 else 0 END) +
                (CASE WHEN old.comment != new.comment then 2048 else 0 END) +
                (CASE WHEN old.salary != new.salary then 4096 else 0 END) +
                (CASE WHEN old.electricity_cost != new.electricity_cost then 8192 else 0 END) +
                
                (CASE WHEN old.last_update != new.last_update then 16384 else 0 END) 
            WHERE 
                old.ID_tblResource != new.ID_tblResource OR
                old.FK_tblResources_tblCatalogs != new.FK_tblResources_tblCatalogs OR
                old.period != new.period OR
                old.code != new.code OR
                old.description != new.description OR
                old.measurer != new.measurer OR
                old.okp != new.okp OR
                old.okpd2 != new.okpd2 OR
                old.base_price != new.base_price OR
                
                old.netto != new.netto OR
                old.brutto != new.brutto OR
                old.comment != new.comment OR
                old.salary != new.salary OR
                old.electricity_cost != new.electricity_cost OR
                
                old.last_update != new.last_update;    
        END;
    """,


    "delete_table_resources": """DROP TABLE IF EXISTS tblResources;""",
    "delete_index_resources": """DROP INDEX IF EXISTS idxResources;""",
    "delete_table_history_resources": """DROP TABLE IF EXISTS _tblHistoryResources;""",
    "delete_index_history_resources": """DROP INDEX IF EXISTS idxHistoryResources;""",

}

