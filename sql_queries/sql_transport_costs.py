


sql_transport_costs = {
    "delete_table_transport_costs": """DROP TABLE IF EXISTS tblTransportCosts;""",
    "delete_view_transport_costs": """DROP VIEW IF EXISTS viewTransportCosts;""",
    "delete_table_history_transport_costs": """DROP TABLE IF EXISTS _tblHistoryTransportCosts;""",

    "insert_transport_cost": """--sql
        INSERT INTO tblTransportCosts
            (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods, name, description, base_price, actual_price)
        VALUES
            (?, ?, ?, ?, ?, ?);
    """,
    "update_transport_cost_id": """--sql
        UPDATE tblTransportCosts
        SET
            FK_tblTransportCosts_tblProducts = ?, FK_tblTransportCosts_tblPeriods = ?,
            base_price = ?, actual_price = ?, description = ?
        WHERE ID_tblTransportCost = ?;
    """,
    "create_table_transport_costs": """--sql
        -- таблица Транспортных расходов
        CREATE TABLE tblTransportCosts
        (
            ID_tblTransportCost                 INTEGER PRIMARY KEY NOT NULL,
            FK_tblTransportCosts_tblProducts    INTEGER NOT NULL, -- id продукта
            FK_tblTransportCosts_tblPeriods     INTEGER NOT NULL, -- id периода
            --
            base_price      REAL DEFAULT 0 NOT NULL,    -- БЦ: базовая цена перевозки 1 т. груза
            actual_price    REAL DEFAULT 0 NOT NULL,    -- ТЦ: текущая цена перевозки 1 т. груза
            numeric_ratio   REAL DEFAULT 1 NOT NULL,    -- коэффициент, применяется после сложения
            description     TEXT,                       -- описание
            inflation_ratio REAL GENERATED ALWAYS AS (
                CASE WHEN base_price = 0 THEN 0 ELSE ROUND(actual_price / base_price, 2) END
            ) VIRTUAL,
            last_update     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
            --
            FOREIGN KEY (FK_tblTransportCosts_tblProducts) REFERENCES tblProducts (ID_Product),
            FOREIGN KEY (FK_tblTransportCosts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
            --
            UNIQUE (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods)
        );
    """,
    "create_index_transport_costs": """--sql
        CREATE UNIQUE INDEX idxTransportCosts ON tblTransportCosts (
            FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods
        );
    """,
    "create_view_transport_costs": """--sql
        CREATE VIEW viewTransportCosts AS
            SELECT
                per.title AS 'период',
                p.code AS 'шифр',
                p.description AS 'название',
                tc.base_price AS 'базовая цена',
                tc.actual_price AS 'текущая цена',
                tc.inflation_ratio AS 'инфл.коэф',
                tc.numeric_ratio AS 'коэф ?',
                tc.description AS 'описание'
            FROM tblTransportCosts tc
            LEFT JOIN tblProducts AS p ON p.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
            LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            ORDER BY tc.FK_tblTransportCosts_tblPeriods, p.digit_code;
    """,
    # ---------------------------------------------------------------------------------------------
    "create_table_history_transport_costs": """--sql
        -- таблица для хранения историиТранспортных расходов
        CREATE TABLE _tblHistoryTransportCosts (
            _rowid                           INTEGER,
            ID_tblTransportCost              INTEGER,
            FK_tblTransportCosts_tblProducts INTEGER,
            FK_tblTransportCosts_tblPeriods  INTEGER,
            base_price                       REAL,
            actual_price                     REAL,
            numeric_ratio                    REAL,
            description                      TEXT,
            last_update                      INTEGER,
            _version                         INTEGER NOT NULL,
            _updated                         INTEGER NOT NULL,
            _mask                            INTEGER NOT NULL
        );
        """,
    "create_index_history_transport_costs": """--sql
        CREATE INDEX idxHistoryTransportCosts ON _tblHistoryTransportCosts (_rowid);
    """,
    "create_trigger_insert_transport_costs": """--sql
        CREATE TRIGGER tgrHistoryTransportCostsInsert
        AFTER INSERT ON tblTransportCosts
        BEGIN
            INSERT INTO _tblHistoryTransportCosts (
                _rowid, ID_tblTransportCost,
                FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods,
                base_price, actual_price, numeric_ratio, description, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblTransportCost,
                new.FK_tblTransportCosts_tblProducts, new.FK_tblTransportCosts_tblPeriods,
                new.base_price, new.actual_price, new.numeric_ratio, new.description,
                new.last_update, 1, unixepoch('now'), 0
            );
        END;
    """,
    "create_trigger_delete_transport_costs": """--sql
        CREATE TRIGGER tgrHistoryTransportCostsDelete
        AFTER DELETE ON tblTransportCosts
        BEGIN
            INSERT INTO _tblHistoryTransportCosts (
                _rowid, ID_tblTransportCost,
                FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods,
                base_price, actual_price, numeric_ratio, description, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblTransportCost,
                old.FK_tblTransportCosts_tblProducts, old.FK_tblTransportCosts_tblPeriods,
                old.base_price, old.actual_price, old.numeric_ratio, old.description, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryTransportCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'), -1
            );
        END;
    """,
    "create_trigger_update_transport_costs": """--sql
        CREATE TRIGGER tgrHistoryTransportCostsUpdate
        AFTER UPDATE ON tblTransportCosts
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryTransportCosts (
                _rowid, ID_tblTransportCost,
                FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods,
                base_price, actual_price, numeric_ratio, description, last_update,
                _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblTransportCost != new.ID_tblTransportCost THEN new.ID_tblTransportCost ELSE null END,
                CASE WHEN old.FK_tblTransportCosts_tblProducts != new.FK_tblTransportCosts_tblProducts THEN new.FK_tblTransportCosts_tblProducts ELSE null END,
                CASE WHEN old.FK_tblTransportCosts_tblPeriods != new.FK_tblTransportCosts_tblPeriods THEN new.FK_tblTransportCosts_tblPeriods ELSE null END,
                CASE WHEN old.base_price != new.base_price THEN new.base_price ELSE null END,
                CASE WHEN old.actual_price != new.actual_price THEN new.actual_price ELSE null END,
                CASE WHEN old.numeric_ratio != new.numeric_ratio THEN new.numeric_ratio ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryTransportCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'),
                (CASE WHEN old.ID_tblTransportCost != new.ID_tblTransportCost then 1 else 0 END) +
                (CASE WHEN old.FK_tblTransportCosts_tblProducts != new.FK_tblTransportCosts_tblProducts then 2 else 0 END) +
                (CASE WHEN old.FK_tblTransportCosts_tblPeriods != new.FK_tblTransportCosts_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.base_price != new.base_price then 8 else 0 END) +
                (CASE WHEN old.actual_price != new.actual_price then 16 else 0 END) +
                (CASE WHEN old.numeric_ratio != new.numeric_ratio then 32 else 0 END) +
                (CASE WHEN old.description != new.description then 64 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 128 else 0 END)
            WHERE
                old.ID_tblTransportCost != new.ID_tblTransportCost OR
                old.FK_tblTransportCosts_tblProducts != new.FK_tblTransportCosts_tblProducts OR
                old.FK_tblTransportCosts_tblPeriods != new.FK_tblTransportCosts_tblPeriods OR
                old.base_price != new.base_price OR
                old.actual_price != new.actual_price OR
                old.numeric_ratio != new.numeric_ratio OR
                old.description != new.description OR
                old.last_update != new.last_update;
        END;
    """,
}


