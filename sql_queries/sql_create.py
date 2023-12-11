sql_creates = {

    # --- > Справочник типов элементов каталога ------------------------------
    # """
    #     CREATE TABLE tbEnumeration (
    #     id_tblDirectory INTEGER PRIMARY KEY NOT NULL,
    #     id_parent INTEGER references tbEnumeration (id_tblDirectory) DEFAULT NULL,
    #     code TEXT NOT NULL,
    #     name TEXT NOT NULL,
    #     unique (code)
    #     );
    # """


    "create_table_catalog_items": """
        CREATE TABLE IF NOT EXISTS tblCatalogItems (
                ID_tblCatalogItem   INTEGER PRIMARY KEY NOT NULL,
                ID_parent           INTEGER REFERENCES tblCatalogItems (ID_tblCatalogItem) DEFAULT NULL,
                name       		    TEXT NOT NULL,
                eng_name    	    TEXT NOT NULL,
                parent_item         
                rank                INTEGER NOT NULL,
                UNIQUE (name)
        );
    """,

    "create_index_catalog_items": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxCatalogItemsName ON tblCatalogItems (name);
    """,


    #
    # --- > Расценки ----------------------------------------------------------
    #
    "drop_table_quotes": """DROP TABLE tblQuotes;""",
    # absolute_code: полный шифр таблица + номер расценки
    # parent_quote: родительская расценка
    "create_table_quotes": """
        CREATE TABLE IF NOT EXISTS tblQuotes (
            ID_tblQuote              INTEGER PRIMARY KEY NOT NULL,
            last_update              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            period                   INTEGER NOT NULL,
            code                     TEXT NOT NULL,
            description              TEXT NOT NULL,
            measure                  TEXT NOT NULL,
            absolute_code            TEXT NOT NULL, 
            parent_quote             INTEGER REFERENCES tblQuotes (ID_tblQuote),
            FK_tblQuotes_tblCatalogs    INTEGER DEFAULT NULL,
            FOREIGN KEY (FK_tblQuotes_tblCatalogs) REFERENCES tblCatalogs (ID_tblCatalog),
            UNIQUE (code)
        );
        """,

    "create_index_quotes": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_code_tblQuotes
        ON tblQuotes (code);
        """,

    # --- > для хранения истории изменений Расценок ---------------------------
    # _mask битовая маска указываюзая какая колонка была измениена. Вычисляется как сложение вместе следующих значений:
    # 1:  ID_tblQuote
    # 2:  last_update
    # 4:  period
    # 8:  code
    # 16: description
    # 32: measure
    # _mask равная -1 показывает что запись была удалена.
    "create_table_history_quotes": """
        CREATE TABLE IF NOT EXISTS _tblQuotesHistory (
            _rowid          INTEGER,
            ID_tblQuote     INTEGER,
            last_update     TIMESTAMP,
            period          INTEGER,
            code            TEXT,
            description     TEXT,
            measure         TEXT,
            absolute_code   TEXT, 
            parent_quote    INTEGER,
            FK_tblQuotes_tblCatalogs INTEGER,
            _version        INTEGER NOT NULL,
            _updated        INTEGER NOT NULL,
            _mask           INTEGER NOT NULL
        );
        """,

    "create_index_quotes_history": """
        CREATE INDEX IF NOT EXISTS idx_rowid_quotes_history
        ON _tblQuotesHistory (_rowid);
        """,

    "create_trigger_insert_history_quotes": """
        CREATE TRIGGER IF NOT EXISTS tgr_insert_tblQuotes
        AFTER INSERT ON tblQuotes
        BEGIN
            INSERT INTO _tblQuotesHistory (
                _rowid, ID_tblQuote, last_update, period, code, description, measure,
                absolute_code, parent_quote, FK_tblQuotes_tblCatalogs,
                _version, _updated, _mask
                )
            VALUES (
                new.rowid, new.ID_tblQuote, new.last_update,
                new.period, new.code, new.description, new.measure,
                new.absolute_code, new.parent_quote, new.FK_tblQuotes_tblCatalogs,
                1, unixepoch('now'), 0
                );
        END;
    """,

    "create_trigger_delete_history_quotes": """
        CREATE TRIGGER tgr_delete_tblQuotes
        AFTER DELETE ON tblQuotes
        BEGIN
            INSERT INTO _tblQuotesHistory (
                _rowid, ID_tblQuote, last_update, period, code, description, measure, absolute_code, parent_quote,
                FK_tblQuotes_tblCatalogs, _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblQuote, old.last_update, old.period, old.code, old.description, old.measure, 
                old.absolute_code, old.parent_quote, old.FK_tblQuotes_tblCatalogs,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblQuotesHistory WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_update_history_quotes": """
        CREATE TRIGGER IF NOT EXISTS tgr_update_tblQuotes
        AFTER UPDATE ON tblQuotes
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblQuotesHistory (
                _rowid, ID_tblQuote, last_update, period, code, description, measure, absolute_code, parent_quote,
                FK_tblQuotes_tblCatalogs, _version, _updated, _mask
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblQuote != new.ID_tblQuote THEN new.ID_tblQuote ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.measure != new.measure THEN new.measure ELSE null END,
                CASE WHEN old.absolute_code != new.absolute_code THEN new.absolute_code ELSE null END,
                CASE WHEN old.parent_quote != new.parent_quote THEN new.parent_quote ELSE null END,
                CASE WHEN old.FK_tblQuotes_tblCatalogs != new.FK_tblQuotes_tblCatalogs 
                    THEN new.FK_tblQuotes_tblCatalogs ELSE null END,
                (SELECT MAX(_version) FROM _tblQuotesHistory WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                (CASE WHEN old.ID_tblQuote != new.ID_tblQuote then 1 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 2 else 0 END) +
                (CASE WHEN old.period != new.period then 4 else 0 END) + 
                (CASE WHEN old.code != new.code then 8 else 0 END) +
                (CASE WHEN old.description != new.description then 16 else 0 END) +
                (CASE WHEN old.measure != new.measure then 32 else 0 END) +
                (CASE WHEN old.absolute_code != new.absolute_code then 64 else 0 END) +
                (CASE WHEN old.parent_quote != new.parent_quote then 128 else 0 END) +
                (CASE WHEN old.FK_tblQuotes_tblCatalogs != new.FK_tblQuotes_tblCatalogs then 256 else 0 END)
            WHERE 
                old.ID_tblQuote != new.ID_tblQuote OR 
                old.last_update != new.last_update OR 
                old.period != new.period OR 
                old.code != new.code OR 
                old.description != new.description OR 
                old.measure != new.measure OR
                old.absolute_code != new.absolute_code OR
                old.parent_quote != new.parent_quote OR 
                old.FK_tblQuotes_tblCatalogs != new.FK_tblQuotes_tblCatalogs;
        END;
    """,


    # --- > Каталог ---------------------------------------------------------------------------
    "create_table_catalogs": """
        CREATE TABLE IF NOT EXISTS tblCatalogs
            (
                ID_tblCatalog			        INTEGER PRIMARY KEY NOT NULL,
                period                 	        INTEGER NOT NULL,
                code	 				        TEXT NOT NULL,								
                description				        TEXT NOT NULL,
                raw_parent                      TEXT NOT NULL,
                ID_parent                       INTEGER REFERENCES tblCatalogs (ID_tblCatalog), 
                FK_tblCatalogs_tblCatalogItems  INTEGER NOT NULL,	
                FOREIGN KEY (FK_tblCatalogs_tblCatalogItems) REFERENCES tblCatalogItems(ID_tblCatalogItem),
                UNIQUE (code)
            );
        """,

    "create_index_code_catalog": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxCatalogCode ON tblCatalogs (code);
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

    # --- > для Хранения подчиненности расценок Основная/Дополнительная расценка ---------

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
        CREATE INDEX IF NOT EXISTS idxChild_tblQuotesChain ON tblQuotesChains (FK_tblQuotes_child);
        """,



}

