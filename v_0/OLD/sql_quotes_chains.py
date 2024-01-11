sql_quotes_chain_create = {

    "delete_table_quotes_chains": """DROP TABLE IF EXISTS tblQuotesChains;""",
    "delete_index_quotes_chains": """DROP INDEX IF EXISTS idx_tblQuotesChains;""",

    "delete_quotes_history_quotes_chains": """DROP TABLE IF EXISTS _tblHistoryQuotesChains;""",
    "delete_index_history_quotes_chains": """DROP INDEX IF EXISTS idxHistoryQuotesChains;""",

    # --- > Хранение подчиненности расценок Основная/Дополнительная расценка ---------

    "create_table_quotes_chains": """
       CREATE TABLE IF NOT EXISTS tblQuotesChains (
            ID_tblQuotesChain   INTEGER PRIMARY KEY NOT NULL,
            last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            period              INTEGER NOT NULL,
            FK_tblQuotes_child  INTEGER DEFAULT NULL,                   -- подчиненная расценка
            FK_tblQuotes_parent INTEGER DEFAULT NULL,                   -- родительская расценка
            
            FOREIGN KEY (FK_tblQuotes_child) REFERENCES tblQuotes (ID_tblQuote),
            FOREIGN KEY (FK_tblQuotes_parent) REFERENCES tblQuotes (ID_tblQuote),
            
            UNIQUE (FK_tblQuotes_child, FK_tblQuotes_parent)
        );
        """,

    "create_index_quotes_chain": """
        CREATE INDEX IF NOT EXISTS idx_tblQuotesChains ON tblQuotesChains (FK_tblQuotes_child, FK_tblQuotes_parent);
        """,

    # --- > История хранения ------------------------------------------------------------

    "create_table_history_quotes_chains": """
        CREATE TABLE IF NOT EXISTS _tblHistoryQuotesChains (
            _rowid              INTEGER NOT NULL,

            ID_tblQuotesChain   INTEGER,
            last_update         INTEGER,
            period              INTEGER,
            FK_tblQuotes_child  INTEGER,                   
            FK_tblQuotes_parent INTEGER,                     
            _version            INTEGER NOT NULL,
            _updated            INTEGER NOT NULL,
            _mask               INTEGER NOT NULL
        );
        """,

    "create_index_history_quotes_chain": """
        CREATE INDEX IF NOT EXISTS idxHistoryQuotesChains ON _tblHistoryQuotesChains (_rowid);
    """,

    # --- > Триггеры --------------------------------------------------------------------

    "create_trigger_history_quotes_chain_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryQuotesChainInsert
        AFTER INSERT ON tblQuotesChains
        BEGIN
            INSERT INTO _tblHistoryQuotesChains (
                _rowid, 
                ID_tblQuotesChain, last_update, period, FK_tblQuotes_child, FK_tblQuotes_parent,
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, 
                new.ID_tblQuotesChain, new.last_update, new.period, new.FK_tblQuotes_child, new.FK_tblQuotes_parent,  
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_quotes_chain_delete": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryQuotesChainDelete
        AFTER DELETE ON tblQuotesChains
        BEGIN
            INSERT INTO _tblHistoryQuotesChains (
                _rowid, 
                ID_tblQuotesChain, last_update, period, FK_tblQuotes_child, FK_tblQuotes_parent,
                _version, 
                _updated, _mask 
            )
            VALUES (
                old.rowid, 
                old.ID_tblQuotesChain, old.last_update, old.period, old.FK_tblQuotes_child, old.FK_tblQuotes_parent,  
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryQuotesChains WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_quotes_chain_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryQuotesChainUpdate
        AFTER UPDATE ON tblQuotesChains
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryQuotesChains (
                _rowid, 
                ID_tblQuotesChain, last_update, period, FK_tblQuotes_child, FK_tblQuotes_parent,
                _version, 
                _updated, 
                _mask 
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblQuotesChain != new.ID_tblQuotesChain THEN new.ID_tblQuotesChain ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                CASE WHEN old.FK_tblQuotes_child != new.FK_tblQuotes_child THEN new.FK_tblQuotes_child ELSE null END,
                CASE WHEN old.FK_tblQuotes_parent != new.FK_tblQuotes_parent THEN new.FK_tblQuotes_parent ELSE null END,
                
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryQuotesChains WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), 
                
                (CASE WHEN old.ID_tblQuotesChain != new.ID_tblQuotesChain then 1 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 2 else 0 END) +
                (CASE WHEN old.period != new.period then 4 else 0 END) +
                (CASE WHEN old.FK_tblQuotes_child != new.FK_tblQuotes_child then 8 else 0 END) +
                (CASE WHEN old.FK_tblQuotes_parent != new.FK_tblQuotes_parent then 16 else 0 END)
            WHERE 
                old.ID_tblQuotesChain != new.ID_tblQuotesChain OR
                old.last_update != new.last_update OR
                old.period != new.period OR
                old.FK_tblQuotes_child != new.FK_tblQuotes_child OR
                old.FK_tblQuotes_parent != new.FK_tblQuotes_parent;    
        END;
    """,




}

