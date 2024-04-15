


sql_transport_costs = {
    # ------------------------------------------------------------------------------------
    "delete_transport_costs_less_max_idex": """--sql
        DELETE FROM tblTransportCosts
        WHERE ID_TransportCost IN (
            SELECT tc.ID_TransportCost
            FROM tblTransportCosts AS tc
            JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE
                per.index_num IS NOT NULL
                AND per.index_num > 0
                AND per.index_num < ?
                AND tc.ID_TransportCost IS NOT NULL
                AND per.ID_tblPeriod IS NOT NULL
        );
    """,
    "select_transport_costs_count_less_index_number": """--sql
        SELECT COUNT() AS number
        FROM tblTransportCosts AS tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE per.index_num IS NOT NULL AND per.index_num > 0 AND per.index_num < ?
        AND tc.FK_tblTransportCosts_tblPeriods IS NOT NULL;
    """,
    "select_transport_costs_max_index_number": """--sql
        SELECT COALESCE(MAX(per.index_num), 0) AS max_index
        FROM tblTransportCosts AS tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE per.index_num IS NOT NULL;
    """,
    # ------------------------------------------------------------------------------------
    "select_transport_cost_by_base_id": """--sql
        SELECT ID_tblTransportCost FROM tblTransportCosts WHERE base_normative_id = ?;
    """,
    "select_history_transport_cost_by_base_id": """--sql
        -- искать в истории
        SELECT ID_tblTransportCost
        FROM _tblHistoryTransportCosts
        WHERE base_normative_id = ?;
    """,
    "select_transport_cost_by_product_id": """--sql
        SELECT p.index_num AS index_num, tc.*
        FROM tblTransportCosts tc
        JOIN tblPeriods p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE tc.FK_tblTransportCosts_tblProducts = ?;
    """,
    "delete_table_transport_costs": """DROP TABLE IF EXISTS tblTransportCosts;""",
    "delete_view_transport_costs": """DROP VIEW IF EXISTS viewTransportCosts;""",
    "delete_table_history_transport_costs": """DROP TABLE IF EXISTS _tblHistoryTransportCosts;""",
    "insert_transport_cost": """--sql
        INSERT INTO tblTransportCosts
            (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods,
                base_price, actual_price, numeric_ratio, base_normative_id)
        VALUES
            (?, ?, ?, ?, ?, ?);
    """,
    "update_transport_cost_id": """--sql
        UPDATE tblTransportCosts
        SET
            FK_tblTransportCosts_tblProducts = ?, FK_tblTransportCosts_tblPeriods = ?,
            base_price = ?, actual_price = ?, numeric_ratio = ?, base_normative_id = ?
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
            inflation_ratio REAL GENERATED ALWAYS AS (
                CASE WHEN base_price = 0 THEN 0 ELSE ROUND(actual_price / base_price, 2) END
            ) VIRTUAL,
            last_update         INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
            --
            base_normative_id   INTEGER,            -- id из основной бд (Postgres Normative)
            --
            FOREIGN KEY (FK_tblTransportCosts_tblProducts) REFERENCES tblProducts (ID_tblProduct),
            FOREIGN KEY (FK_tblTransportCosts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
            --
            UNIQUE (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods)
        );
    """,
    "create_index_transport_costs": """--sql
        CREATE INDEX idxTransportCosts ON tblTransportCosts (
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
                tc.base_normative_id AS 'base_id'
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
            last_update                      INTEGER,
            base_normative_id                INTEGER,
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
                base_price, actual_price, numeric_ratio, last_update, base_normative_id,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblTransportCost,
                new.FK_tblTransportCosts_tblProducts, new.FK_tblTransportCosts_tblPeriods,
                new.base_price, new.actual_price, new.numeric_ratio, new.last_update,
                new.base_normative_id,
                1, unixepoch('now'), 0
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
                base_price, actual_price, numeric_ratio, last_update, base_normative_id,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblTransportCost,
                old.FK_tblTransportCosts_tblProducts, old.FK_tblTransportCosts_tblPeriods,
                old.base_price, old.actual_price, old.numeric_ratio, old.last_update, old.base_normative_id,
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
                base_price, actual_price, numeric_ratio, last_update, base_normative_id,
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
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                CASE WHEN old.base_normative_id != new.base_normative_id THEN new.base_normative_id ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryTransportCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'),
                (CASE WHEN old.ID_tblTransportCost != new.ID_tblTransportCost then 1 else 0 END) +
                (CASE WHEN old.FK_tblTransportCosts_tblProducts != new.FK_tblTransportCosts_tblProducts then 2 else 0 END) +
                (CASE WHEN old.FK_tblTransportCosts_tblPeriods != new.FK_tblTransportCosts_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.base_price != new.base_price then 8 else 0 END) +
                (CASE WHEN old.actual_price != new.actual_price then 16 else 0 END) +
                (CASE WHEN old.numeric_ratio != new.numeric_ratio then 32 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 64 else 0 END) +
                (CASE WHEN old.base_normative_id != new.base_normative_id then 128 else 0 END)
            WHERE
                old.ID_tblTransportCost != new.ID_tblTransportCost OR
                old.FK_tblTransportCosts_tblProducts != new.FK_tblTransportCosts_tblProducts OR
                old.FK_tblTransportCosts_tblPeriods != new.FK_tblTransportCosts_tblPeriods OR
                old.base_price != new.base_price OR
                old.actual_price != new.actual_price OR
                old.numeric_ratio != new.numeric_ratio OR
                old.last_update != new.last_update OR
                old.base_normative_id != new.base_normative_id;
        END;
    """,
}


