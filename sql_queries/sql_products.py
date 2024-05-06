sql_products_queries = {
    "select_history_products_origin_code": """--sql
        SELECT ID_tblProduct FROM _tblHistoryProducts WHERE FK_tblProducts_tblOrigins = ? AND code = ? LIMIT 1;

    """,
    # "delete_products_last_periods": """
    #     DELETE FROM tblProducts
    #     WHERE ID_tblProduct IN (
    #         SELECT ID_tblProduct
    #         FROM tblProducts
    #         WHERE period > 0 AND period < ?
    #     );
    # """,
    "delete_products_origin_item_less_max_supplement": """--sql
        DELETE FROM tblProducts
        WHERE ID_tblProduct IN (
            SELECT m.ID_tblProduct
            FROM tblProducts AS m
            JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
            WHERE
                m.FK_tblProducts_tblOrigins = ?
                AND m.FK_tblProducts_tblItems = ?
                AND (per.supplement_num > 0 AND per.supplement_num < ?)
        );
    """,
    "select_product_id_origin_code": """--sql
        SELECT ID_tblProduct FROM tblProducts WHERE FK_tblProducts_tblOrigins = ? AND code = ?;
    """,
    "select_product_all_code": """--sql
        SELECT * FROM tblProducts WHERE code = ?;
    """,
    "select_products_origin_code": """--sql
        SELECT * FROM tblProducts WHERE FK_tblProducts_tblOrigins = ? AND code = ?;
    """,
    "select_products_id": """--sql
        SELECT * FROM tblProducts WHERE ID_tblProduct = ?;
    """,
    "select_products_max_supplement_origin_item": """--sql
        SELECT MAX(per.supplement_num) AS max_suppl
        FROM tblProducts AS m
        JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
        WHERE m.FK_tblProducts_tblOrigins = ? AND m.FK_tblProducts_tblItems = ?;
    """,
    # "select_products_count_period_less": """
    #     SELECT COUNT(*) FROM tblProducts WHERE WHERE FK_tblProducts_tblOrigins = ? AND (period > 0 AND period < ?);
    # """,
    "select_products_count_origin_item_less_supplement": """--sql
        SELECT COUNT(*) AS number
        FROM tblProducts AS m
        JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
        WHERE
            m.FK_tblProducts_tblOrigins = ? AND
            m.FK_tblProducts_tblItems = ? AND
            (per.supplement_num > 0 AND per.supplement_num < ?);
    """,
    "select_changes": """SELECT CHANGES() AS changes;""",
    # --->
    #               FK_tblProducts_tblCatalogs  INTEGER NOT NULL, -- родитель каталога
    #                 FK_tblProducts_tblItems     INTEGER NOT NULL, -- тип материал/машина/расценка/оборудование
    #                 FK_tblProducts_tblOrigins   INTEGER NOT NULL, -- происхождение ТСН/ПСМ...
    #                 --
    #                 period      INTEGER NOT NULL,   -- период
    #                 code	 	TEXT NOT NULL,		-- шифр
    #                 description TEXT NOT NULL,      -- описание
    #                 measurer    TEXT,               -- единица измерения
    #                 digit_code  INTEGER NOT NULL,   -- шифр преобразованный в число
    "insert_product": """--sql
        INSERT INTO tblProducts (
            FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
            FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods,
            code, description, measurer, digit_code
        )
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    "update_product_id": """--sql
        UPDATE tblProducts
        SET
            FK_tblProducts_tblCatalogs = ?, FK_tblProducts_tblItems = ?,
            FK_tblProducts_tblOrigins = ?, FK_tblProducts_tblPeriods = ?,
            code = ?, description = ?, measurer = ?, digit_code = ?
        WHERE ID_tblProduct = ?;
    """,
    "update_product_period_by_id": """--sql
        UPDATE tblProducts SET (FK_tblProducts_tblPeriods) = (?) WHERE ID_tblProduct = ?;
    """,
}

sql_products_creates = {

    "delete_table_products": """DROP TABLE IF EXISTS tblProducts;""",
    "delete_index_products": """DROP INDEX IF EXISTS idxProductsCode;""",
    "delete_view_products": """DROP VIEW IF EXISTS viewProducts; """,

    "delete_table_products_history": """DROP TABLE IF EXISTS _tblHistoryProducts;""",
    "delete_index_products_history": """DROP INDEX IF EXISTS idxHistoryProducts;""",


    # --- > Базовая таблица для хранения Расценок, Материалов, Машин и Оборудования ----
    "create_table_products": """--sql
        CREATE TABLE IF NOT EXISTS tblProducts
            (
                ID_tblProduct               INTEGER PRIMARY KEY NOT NULL,
                FK_tblProducts_tblCatalogs  INTEGER NOT NULL, -- родитель каталога
                FK_tblProducts_tblItems     INTEGER NOT NULL, -- тип материал/машина/расценка/оборудование
                FK_tblProducts_tblOrigins   INTEGER NOT NULL, -- происхождение ТСН/ПСМ...
                FK_tblProducts_tblPeriods   INTEGER NOT NULL, -- id периода
                --
                code	 	TEXT NOT NULL,		-- шифр
                description TEXT NOT NULL,      -- описание
                measurer    TEXT,               -- единица измерения
                digit_code  INTEGER NOT NULL,   -- шифр преобразованный в число
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),

                FOREIGN KEY (FK_tblProducts_tblCatalogs) REFERENCES tblCatalogs (ID_tblCatalog),
                FOREIGN KEY (FK_tblProducts_tblItems) REFERENCES tblItems (ID_tblItem),
                FOREIGN KEY (FK_tblProducts_tblOrigins) REFERENCES tblOrigins (ID_tblOrigin),
                FOREIGN KEY (FK_tblProducts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
                UNIQUE (FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods, code )
            );
        """,

    "create_index_products": """--sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxProductsCode ON tblProducts (
            code, FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods
            );
    """,

    # --- > История базовой таблицы -----------------------------------------------------
    "create_table_history_products": """--sql
        CREATE TABLE IF NOT EXISTS _tblHistoryProducts (
            _rowid        INTEGER,
            ID_tblProduct INTEGER,
            FK_tblProducts_tblCatalogs INTEGER,
            FK_tblProducts_tblItems INTEGER,
            FK_tblProducts_tblOrigins INTEGER,
            FK_tblProducts_tblPeriods INTEGER,
            code	 	  TEXT,
            description	  TEXT,
            measurer      TEXT,
            digit_code    INTEGER,
            last_update   INTEGER,
            _version      INTEGER NOT NULL,
            _updated      INTEGER NOT NULL,
            _mask         INTEGER NOT NULL
        );
        """,

    "create_index_history_products": """--sql
        CREATE INDEX IF NOT EXISTS idxHistoryProducts ON _tblHistoryProducts (_rowid);
    """,
    # --- > Триггеры базовой таблицы -----------------------------------------------------

    "create_trigger_history_products_insert": """--sql
        CREATE TRIGGER IF NOT EXISTS tgrHistoryProductsInsert
        AFTER INSERT ON tblProducts
        BEGIN
            INSERT INTO _tblHistoryProducts (
                _rowid, ID_tblProduct,
                FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
                FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods,
                code, description, measurer, digit_code, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblProduct,
                new.FK_tblProducts_tblCatalogs, new.FK_tblProducts_tblItems,
                new.FK_tblProducts_tblOrigins, new.FK_tblProducts_tblPeriods,
                new.code, new.description, new.measurer, new.digit_code, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_products_delete": """--sql
        CREATE TRIGGER tgrHistoryProductsDelete
        AFTER DELETE ON tblProducts
        BEGIN
            INSERT INTO _tblHistoryProducts (
                _rowid, ID_tblProduct,
                FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
                FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods,
                code, description, measurer, digit_code, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblProduct,
                old.FK_tblProducts_tblCatalogs, old.FK_tblProducts_tblItems,
                old.FK_tblProducts_tblOrigins, old.FK_tblProducts_tblPeriods,
                old.code, old.description, old.measurer, old.digit_code, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryProducts WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_products_update": """--sql
        CREATE TRIGGER IF NOT EXISTS tgrHistoryProductsUpdate
        AFTER UPDATE ON tblProducts
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryProducts (
                _rowid, ID_tblProduct,
                FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems,
                FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods,
                code, description, measurer, digit_code, last_update,
                _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblProduct != new.ID_tblProduct THEN new.ID_tblProduct ELSE null END,
                CASE WHEN old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs THEN new.FK_tblProducts_tblCatalogs ELSE null END,
                CASE WHEN old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems THEN new.FK_tblProducts_tblItems ELSE null END,
                CASE WHEN old.FK_tblProducts_tblOrigins != new.FK_tblProducts_tblOrigins THEN new.FK_tblProducts_tblOrigins ELSE null END,
                CASE WHEN old.FK_tblProducts_tblPeriods != new.FK_tblProducts_tblPeriods THEN new.FK_tblProducts_tblPeriods ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.measurer != new.measurer THEN new.measurer ELSE null END,
                CASE WHEN old.digit_code != new.digit_code THEN new.digit_code ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,

                (SELECT MAX(_version) FROM _tblHistoryProducts WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),

                (CASE WHEN old.ID_tblProduct != new.ID_tblProduct then 1 else 0 END) +
                (CASE WHEN old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs then 2 else 0 END) +
                (CASE WHEN old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems then 4 else 0 END) +
                (CASE WHEN old.FK_tblProducts_tblOrigins != new.FK_tblProducts_tblOrigins then 8 else 0 END) +
                (CASE WHEN old.FK_tblProducts_tblPeriods != new.FK_tblProducts_tblPeriods then 16 else 0 END) +
                (CASE WHEN old.code != new.code then 32 else 0 END) +
                (CASE WHEN old.description != new.description then 64 else 0 END) +
                (CASE WHEN old.measurer != new.measurer then 128 else 0 END) +
                (CASE WHEN old.digit_code != new.digit_code then 256 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 512 else 0 END)
            WHERE
                old.ID_tblProduct != new.ID_tblProduct OR
                old.FK_tblProducts_tblCatalogs != new.FK_tblProducts_tblCatalogs OR
                old.FK_tblProducts_tblItems != new.FK_tblProducts_tblItems OR
                old.FK_tblProducts_tblOrigins != new.FK_tblProducts_tblOrigins OR
                old.FK_tblProducts_tblPeriods != new.FK_tblProducts_tblPeriods OR
                old.code != new.code OR
                old.description != new.description OR
                old.measurer != new.measurer OR
                old.digit_code != new.digit_code OR
                old.last_update != new.last_update;
        END;
    """,

    "create_view_products": """--sql
        CREATE VIEW viewProducts AS
            SELECT
                per.title AS [period],
                o.name AS origin,
                i.title AS product_type,
                c.code AS parent_code,
                m.code AS code,
                m.description AS title,
                m.measurer AS measurer
            FROM tblProducts m
            LEFT JOIN tblCatalogs AS c ON c.ID_tblCatalog = m.FK_tblProducts_tblCatalogs
            LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = m.FK_tblProducts_tblOrigins
            LEFT JOIN tblItems AS i ON i.ID_tblItem = m.FK_tblProducts_tblItems
            LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
            ORDER BY m.digit_code;

    """,

}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
