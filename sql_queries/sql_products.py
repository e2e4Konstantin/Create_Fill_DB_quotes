
sql_products_queries = {

    "delete_quotes_last_periods": """
        DELETE FROM tblQuotes 
        WHERE ID_tblQuote IN (
                SELECT ID_tblQuote 
                FROM tblQuotes 
                WHERE period > 0 AND period < ?);
        """,

    "select_quotes_code":   """
        SELECT ID_tblQuote FROM tblQuotes WHERE code = ?;
        """,
    "select_products_item_code":   """
        SELECT * FROM tblProducts WHERE FK_tblProducts_tblItems = ? AND code = ?;
    """,

    "select_quotes_max_period": """
        SELECT MAX(period) AS max_period FROM tblQuotes;         
    """,

    "select_quotes_count_period_less": """
        SELECT COUNT(*) FROM tblQuotes WHERE period > 0 AND period < ?;
    """,

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



sql_products_creates = {

    "delete_table_products": """DROP TABLE IF EXISTS tblProducts;""",
    "delete_index_products": """DROP INDEX IF EXISTS idxProductsCode;""",
    "delete_view_products": """DROP VIEW IF EXISTS viewProducts; """,

    "delete_table_products_history": """DROP TABLE IF EXISTS _tblHistoryProducts;""",
    "delete_index_products_history": """DROP INDEX IF EXISTS idxHistoryProducts;""",

    # --- > Базовая таблица для хранения Расценок, Материалов, Машин и Оборудования ----
    "create_table_products": """
        CREATE TABLE IF NOT EXISTS tblProducts
            (
                ID_tblProduct INTEGER PRIMARY KEY NOT NULL,
                FK_tblProducts_tblCatalogs INTEGER NOT NULL,    -- родитель каталога
                FK_tblProducts_tblItems INTEGER NOT NULL,       -- тип элемента из справочника 'units'
                                                                -- материал/машина/расценка/оборудование
                -- 
                period      INTEGER NOT NULL,   -- период
                code	 	TEXT NOT NULL,		-- шифр					
                description TEXT NOT NULL,      -- описание
                measurer    TEXT,               -- единица измерения
                full_code   TEXT,               -- полный шифр    
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                
                FOREIGN KEY (FK_tblProducts_tblCatalogs) REFERENCES tblCatalogs (ID_tblCatalog),
                FOREIGN KEY (FK_tblProducts_tblItems) REFERENCES tblItems (ID_tblItem),
                UNIQUE (code)
            );
        """,

    "create_index_products": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxProductsCode ON tblProducts (code, period);
    """,


    # --- > История базовой таблицы -----------------------------------------------------
    "create_table_history_products": """
        CREATE TABLE IF NOT EXISTS _tblHistoryProducts (
            _rowid        INTEGER,
            ID_tblProduct INTEGER,
            FK_tblProducts_tblCatalogs INTEGER, 
            FK_tblProducts_tblItems INTEGER,
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

    "create_index_history_products": """
        CREATE INDEX IF NOT EXISTS idxHistoryProducts ON _tblHistoryProducts (_rowid);
    """,
    # --- > Триггеры базовой таблицы -----------------------------------------------------

    "create_trigger_history_products_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryProductsInsert
        AFTER INSERT ON tblProducts
        BEGIN
            INSERT INTO _tblHistoryProducts (
                _rowid, ID_tblProduct, FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, 
                period, code, description, measurer, full_code, last_update,
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_tblProduct, new.FK_tblProducts_tblCatalogs, new.FK_tblProducts_tblItems, 
                new.period, new.code, new.description, new.measurer, new.full_code, new.last_update, 
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_products_delete": """
        CREATE TRIGGER tgrHistoryProductsDelete
        AFTER DELETE ON tblProducts
        BEGIN
            INSERT INTO _tblHistoryProducts (
                _rowid, ID_tblProduct, FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, 
                period, code, description, measurer, full_code, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblProduct, old.FK_tblProducts_tblCatalogs, old.FK_tblProducts_tblItems, 
                old.period, old.code, old.description, old.measurer, old.full_code, old.last_update, 
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryProducts WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_products_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryProductsUpdate
        AFTER UPDATE ON tblProducts
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryProducts (
                _rowid, ID_tblProduct, FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, 
                period, code, description, measurer, full_code, last_update,
                _version, _updated, _mask
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblProduct != new.ID_tblProduct THEN new.ID_tblProduct ELSE null END,
                CASE WHEN old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs THEN new.FK_tblProducts_tblCatalogs ELSE null END,
                CASE WHEN old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems THEN new.FK_tblProducts_tblItems ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.measurer != new.measurer THEN new.measurer ELSE null END,
                CASE WHEN old.full_code != new.full_code THEN new.full_code ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                
                (SELECT MAX(_version) FROM _tblHistoryProducts WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                
                (CASE WHEN old.ID_tblProduct != new.ID_tblProduct then 1 else 0 END) +
                (CASE WHEN old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs then 2 else 0 END) +
                (CASE WHEN old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems then 4 else 0 END) +
                (CASE WHEN old.period != new.period then 8 else 0 END) +
                (CASE WHEN old.code != new.code then 16 else 0 END) +
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.measurer != new.measurer then 64 else 0 END) +
                (CASE WHEN old.full_code != new.full_code then 128 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 256 else 0 END)
            WHERE 
                old.ID_tblProduct != new.ID_tblProduct OR
                old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs OR
                old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems OR
                old.period != new.period OR
                old.code != new.code OR
                old.description != new.description OR
                old.measurer != new.measurer OR
                old.full_code != new.full_code OR
                old.last_update != new.last_update;
        END;
    """,

    "create_view_products": """
        CREATE VIEW viewProducts AS
            SELECT 
                i.title AS parent,
                c.code AS parent_code,
                p.code AS code,
                p.description AS title,
                p.measurer AS measurer
            FROM tblProducts p 
            LEFT JOIN tblCatalogs AS c ON c.ID_tblCatalog = b.FK_tblProducts_tblCatalogs
            LEFT JOIN tblItems AS i ON i.ID_tblItem = c.FK_tblProducts_tblItems
            ORDER BY quotes_code;
    """,



}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
