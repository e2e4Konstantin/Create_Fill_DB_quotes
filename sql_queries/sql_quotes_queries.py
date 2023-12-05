sql_quotes_delete = {
    "delete_quotes_last_periods": """
        DELETE FROM tblQuotes 
        WHERE ID_tblQuote IN (
                SELECT ID_tblQuote 
                FROM tblQuotes 
                WHERE period > 0 AND period < ?);
        """,
}



sql_quotes_select = {
    "select_quotes_code":   """
        SELECT ID_tblQuote FROM tblQuotes WHERE code = ?;
        """,
    "select_quotes_row_code":   """
        SELECT * FROM tblQuotes WHERE code = ?;
        """,
    "select_quotes_max_period": """
        SELECT MAX(period) AS max_period FROM tblQuotes;         
    """,

    "select_quotes_count_period_less": """
        SELECT COUNT(*) FROM tblQuotes WHERE period > 0 AND period < ?;
    """,
}



sql_quotes_insert_update = {
    "insert_quote": """
        INSERT INTO tblQuotes (
                                    FK_tblQuotes_tblCatalogs, period, code, description, measurer, 
                                    salary, operation_of_machines, cost_of_material
                                 ) 
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?);
    """,

    "update_quote_id": """
        UPDATE tblQuotes 
        SET 
            FK_tblQuotes_tblCatalogs = ?, 
            period = ?, code = ?, description = ?, measurer = ?, 
            salary = ?, operation_of_machines = ?, cost_of_material = ? 
        WHERE ID_tblQuote = ?;
    """,
}

sql_quotes_creates = {

    # --- > Расценки ---------------------------------------------------------------------------
    "create_table_quotes": """
        CREATE TABLE IF NOT EXISTS tblQuotes
            (
                ID_tblQuote   INTEGER PRIMARY KEY NOT NULL,
                --
                FK_tblQuotes_tblCatalogs INTEGER NOT NULL, -- id материнского элемента каталога
                ID_parent     INTEGER REFERENCES tblQuotes (ID_tblQuote) DEFAULT NULL, -- id родительской расценки
                -- 
                period        INTEGER NOT NULL,         -- период
                code	 	  TEXT NOT NULL,			-- шифр					
                description	  TEXT NOT NULL,            -- описание
                measurer      TEXT DEFAULT '',          -- единица измерения
                --
                salary	              REAL DEFAULT 0,   -- фонд оплаты труда
                operation_of_machines REAL DEFAULT 0,	-- стоимость эксплуатации машин
                cost_of_material      REAL DEFAULT 0,	-- стоимость материалов
                direct_costs REAL GENERATED ALWAYS AS (
                        salary + operation_of_machines + cost_of_material
                    ) STORED,                           -- прямые затраты
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                FOREIGN KEY (FK_tblQuotes_tblCatalogs) REFERENCES tblCatalogs (ID_tblCatalog),
                UNIQUE (code)
            );
        """,

    "create_index_quotes": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxQuotesCode ON tblQuotes (code, period);
    """,

    "delete_table_quotes": """DROP TABLE IF EXISTS tblQuotes;""",
    "delete_index_quotes": """DROP INDEX IF EXISTS idxQuotesCode;""",

    # --- > История Расценок ----------------------------------------------------------------------
    "create_table_history_quotes": """
        CREATE TABLE IF NOT EXISTS _tblHistoryQuotes (
            _rowid        INTEGER,
            
            ID_tblQuote   INTEGER,
            FK_tblQuotes_tblCatalogs INTEGER,
            ID_parent     INTEGER,
            period        INTEGER,
            code	 	  TEXT,
            description	  TEXT,
            measurer      TEXT,
            salary	                REAL,
            operation_of_machines   REAL,
            cost_of_material        REAL,
            direct_costs 	        REAL,                         
            last_update             INTEGER,          
            
            _version      INTEGER NOT NULL,
            _updated      INTEGER NOT NULL,
            _mask         INTEGER NOT NULL
        );
        """,

    "create_index_history_quotes": """
        CREATE INDEX IF NOT EXISTS idxHistoryQuotes ON _tblHistoryQuotes (_rowid);
    """,

    "create_trigger_history_quotes_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryQuotesInsert
        AFTER INSERT ON tblQuotes
        BEGIN
            INSERT INTO _tblHistoryQuotes (
                _rowid, ID_tblQuote, FK_tblQuotes_tblCatalogs, ID_parent, 
                period, code, description, measurer, 
                salary, operation_of_machines, cost_of_material, direct_costs, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_tblQuote, new.FK_tblQuotes_tblCatalogs, new.ID_parent, 
                new.period, new.code, new.description, new.measurer, 
                new.salary, new.operation_of_machines, new.cost_of_material, new.direct_costs, new.last_update,  
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_quotes_delete": """
        CREATE TRIGGER tgrHistoryQuotesDelete
        AFTER DELETE ON tblQuotes
        BEGIN
            INSERT INTO _tblHistoryQuotes (
                _rowid, ID_tblQuote, FK_tblQuotes_tblCatalogs, ID_parent, 
                period, code, description, measurer, 
                salary, operation_of_machines, cost_of_material, direct_costs, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                old.rowid, old.ID_tblQuote, old.FK_tblQuotes_tblCatalogs, old.ID_parent, 
                old.period, old.code, old.description, old.measurer, 
                old.salary, old.operation_of_machines, old.cost_of_material, old.direct_costs, old.last_update, 
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryQuotes WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_quotes_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryQuotesUpdate
        AFTER UPDATE ON tblQuotes
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryQuotes (
                _rowid, ID_tblQuote, FK_tblQuotes_tblCatalogs, ID_parent, 
                period, code, description, measurer, 
                salary, operation_of_machines, cost_of_material, direct_costs, last_update, 
                _version, _updated, _mask  
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblQuote != new.ID_tblQuote THEN new.ID_tblQuote ELSE null END,
                CASE WHEN old.FK_tblQuotes_tblCatalogs != new.FK_tblQuotes_tblCatalogs THEN new.FK_tblQuotes_tblCatalogs ELSE null END,
                CASE WHEN old.ID_parent != new.ID_parent THEN new.ID_parent ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.measurer != new.measurer THEN new.measurer ELSE null END,
                
                CASE WHEN old.salary != new.salary THEN new.salary ELSE null END,
                CASE WHEN old.operation_of_machines != new.operation_of_machines THEN new.operation_of_machines ELSE null END,
                CASE WHEN old.cost_of_material != new.cost_of_material THEN new.cost_of_material ELSE null END,
                CASE WHEN old.direct_costs != new.direct_costs THEN new.direct_costs ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                
                (SELECT MAX(_version) FROM _tblHistoryQuotes WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                
                (CASE WHEN old.ID_tblQuote != new.ID_tblQuote then 1 else 0 END) +
                (CASE WHEN old.FK_tblQuotes_tblCatalogs != new.FK_tblQuotes_tblCatalogs then 2 else 0 END) +
                (CASE WHEN old.ID_parent != new.ID_parent then 4 else 0 END) +
                (CASE WHEN old.period != new.period then 8 else 0 END) +
                (CASE WHEN old.code != new.code then 16 else 0 END) +
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.measurer != new.measurer then 64 else 0 END) +
                
                (CASE WHEN old.salary != new.salary then 128 else 0 END) +
                (CASE WHEN old.operation_of_machines != new.operation_of_machines then 256 else 0 END) +
                (CASE WHEN old.cost_of_material != new.cost_of_material then 512 else 0 END) +
                (CASE WHEN old.direct_costs != new.direct_costs then 1024 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 2048 else 0 END)
            WHERE 
                old.ID_tblQuote != new.ID_tblQuote OR
                old.FK_tblQuotes_tblCatalogs != new.FK_tblQuotes_tblCatalogs OR
                old.ID_parent != new.ID_parent OR
                old.period != new.period OR
                old.code != new.code OR
                old.description != new.description OR
                old.measurer != new.measurer OR
                
                old.salary != new.salary OR
                old.operation_of_machines != new.operation_of_machines OR
                old.cost_of_material != new.cost_of_material OR
                old.direct_costs != new.direct_costs OR
                   
                old.last_update != new.last_update;
        END;
    """,

    "create_view_catalog_main": """
        CREATE VIEW viewCatalog AS
            SELECT 
                m.period AS 'период',
                i.name AS 'тип', 
                m.code AS 'шифр', 
                m.description AS 'описание',
            
                (SELECT i.name
                FROM tblCatalogs p
                LEFT JOIN tblDirectoryItems i ON i.ID_tblDirectoryItem = p.FK_tblCatalogs_tblDirectoryItems
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'тип родителя',
                
                (SELECT p.code
                FROM tblCatalogs p
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'шифр родителя',
                
               (SELECT p.description
                FROM tblCatalogs p
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'описание родителя'
                
            FROM tblCatalogs m 
            LEFT JOIN tblDirectoryItems AS i ON i.ID_tblDirectoryItem = m.FK_tblCatalogs_tblDirectoryItems;
            --ORDER BY m.code
    """,

}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
