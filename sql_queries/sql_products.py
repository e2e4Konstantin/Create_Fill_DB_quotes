sql_products_queries = {

    "delete_products_last_periods": """
        DELETE FROM tblProducts 
        WHERE ID_tblProduct IN (
            SELECT ID_tblProduct 
            FROM tblProducts 
            WHERE period > 0 AND period < ?
        );
    """,

    "delete_products_period_team_name": """
        DELETE FROM tblProducts 
        WHERE ID_tblProduct IN (
            SELECT ID_tblProduct 
            FROM tblProducts AS p
            WHERE 
                p.FK_tblProducts_tblOrigins = ? AND 
                p.FK_tblProducts_tblItems = (SELECT ID_tblItem FROM tblItems AS i WHERE i.team = ? AND i.name = ?) AND
                (p.period > 0 AND p.period < ?)
        );
    """,

    "select_product_id_origin_code": """
        SELECT ID_tblProduct FROM tblProducts WHERE FK_tblProducts_tblOrigins = ? AND code = ?;
    """,

    # "select_product_id_period_code": """
    #     SELECT ID_tblProduct FROM tblProducts
    #     WHERE period = ? AND code = ?;
    # """,

    "select_products_origin_code": """
        SELECT * FROM tblProducts WHERE FK_tblProducts_tblOrigins = ? AND code = ?;
    """,

    "select_products_id": """
        SELECT * FROM tblProducts WHERE ID_tblProduct = ?;
    """,

    "select_products_max_period": """
        SELECT MAX(period) AS max_period FROM tblProducts WHERE FK_tblProducts_tblOrigins = ?;         
    """,

    "select_products_max_period_origin_team_name": """
        SELECT MAX(period) AS max_period 
        FROM tblProducts AS p
        WHERE
            FK_tblProducts_tblOrigins = ? AND 
            p.FK_tblProducts_tblItems = (SELECT ID_tblItem FROM tblItems AS i WHERE i.team = ? AND i.name = ?);
    """,

    "select_products_count_period_less": """
        SELECT COUNT(*) FROM tblProducts WHERE WHERE FK_tblProducts_tblOrigins = ? AND (period > 0 AND period < ?);
    """,

    "select_products_count_origin_team_name_period": """
        SELECT COUNT(*) AS number FROM tblProducts AS p 
        WHERE  
            FK_tblProducts_tblOrigins = ? AND 
            FK_tblProducts_tblItems = (SELECT ID_tblItem FROM tblItems AS i WHERE i.team = ? AND i.name = ?) AND
            (p.period > 0 AND p.period < ?);
    """,

    "select_changes": """SELECT CHANGES() AS changes;""",

    # --->
    "insert_product": """
        INSERT INTO tblProducts (
            FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
            period, code, description, measurer, full_code
        ) 
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?);
    """,

    "update_product_id": """
        UPDATE tblProducts 
        SET 
            FK_tblProducts_tblCatalogs = ?, FK_tblProducts_tblItems = ?, FK_tblProducts_tblOrigins = ?,
            period = ?, code = ?, description = ?, measurer = ?, full_code = ?
        WHERE ID_tblProduct = ?;
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
                ID_tblProduct               INTEGER PRIMARY KEY NOT NULL,
                FK_tblProducts_tblCatalogs  INTEGER NOT NULL, -- родитель каталога
                FK_tblProducts_tblItems     INTEGER NOT NULL, -- тип материал/машина/расценка/оборудование
                FK_tblProducts_tblOrigins   INTEGER NOT NULL, -- происхождение ТСН/ПСМ...                                                                 
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
                FOREIGN KEY (FK_tblProducts_tblOrigins) REFERENCES tblOrigins (ID_tblOrigin),
                UNIQUE (FK_tblProducts_tblOrigins, code)
            );
        """,

    "create_index_products": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxProductsCode ON tblProducts (code, period, FK_tblProducts_tblOrigins);
    """,

    # --- > История базовой таблицы -----------------------------------------------------
    "create_table_history_products": """
        CREATE TABLE IF NOT EXISTS _tblHistoryProducts (
            _rowid        INTEGER,
            ID_tblProduct INTEGER,
            FK_tblProducts_tblCatalogs INTEGER, 
            FK_tblProducts_tblItems INTEGER,
            FK_tblProducts_tblOrigins INTEGER,
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
                _rowid, ID_tblProduct, 
                FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins, 
                period, code, description, measurer, full_code, last_update,
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_tblProduct, 
                new.FK_tblProducts_tblCatalogs, new.FK_tblProducts_tblItems, new.FK_tblProducts_tblOrigins, 
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
                _rowid, ID_tblProduct, 
                FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
                period, code, description, measurer, full_code, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblProduct, 
                old.FK_tblProducts_tblCatalogs, old.FK_tblProducts_tblItems, old.FK_tblProducts_tblOrigins, 
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
                _rowid, ID_tblProduct, 
                FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins, 
                period, code, description, measurer, full_code, last_update,
                _version, _updated, _mask
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblProduct != new.ID_tblProduct THEN new.ID_tblProduct ELSE null END,
                CASE WHEN old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs THEN new.FK_tblProducts_tblCatalogs ELSE null END,
                CASE WHEN old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems THEN new.FK_tblProducts_tblItems ELSE null END,
                CASE WHEN old.FK_tblProducts_tblOrigins != new.FK_tblProducts_tblOrigins THEN new.FK_tblProducts_tblOrigins ELSE null END,
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
                (CASE WHEN old.FK_tblProducts_tblOrigins != new.FK_tblProducts_tblItems then 8 else 0 END) +
                (CASE WHEN old.period != new.period then 16 else 0 END) +
                (CASE WHEN old.code != new.code then 32 else 0 END) +
                (CASE WHEN old.description != new.description then 64 else 0 END) +
                (CASE WHEN old.measurer != new.measurer then 128 else 0 END) +
                (CASE WHEN old.full_code != new.full_code then 256 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 512 else 0 END)
            WHERE 
                old.ID_tblProduct != new.ID_tblProduct OR
                old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs OR
                old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems OR
                old.FK_tblProducts_tblOrigins != new.FK_tblProducts_tblOrigins OR
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
                o.name AS origin,
                i.title AS product_type,
                c.code AS parent_code,
                p.code AS code,
                p.description AS title,
                p.measurer AS measurer
            FROM tblProducts p 
            LEFT JOIN tblCatalogs AS c ON c.ID_tblCatalog = p.FK_tblProducts_tblCatalogs
            LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = p.FK_tblProducts_tblOrigins
            LEFT JOIN tblItems AS i ON i.ID_tblItem = p.FK_tblProducts_tblItems;
            --ORDER BY p.code;
    """,

}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
