sql_quotes_chain_create = {

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
        CREATE INDEX IF NOT EXISTS idx_tblQuotesChain ON tblQuotesChains (FK_tblQuotes_child, FK_tblQuotes_parent);
        """,

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


}

