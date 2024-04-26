sql_monitoring_transport_costs = {
    "select_monitoring_transport_costs_by_product_and_period": """--sql
        SELECT *
        FROM tblMonitoringTransportCosts
        WHERE
            FK_tblMonitoringTransportCosts_tblProducts = ?
            AND FK_tblMonitoringTransportCosts_tblPeriods = ?;
    """,
    #
    "select_monitoring_transport_costs_by_product": """--sql
        SELECT * FROM tblMonitoringTransportCosts WHERE FK_tblMonitoringTransportCosts_tblProducts = ?;
    """,
    #
    "select_monitoring_transport_costs_max_index_number": """--sql
        SELECT MAX(period.index_num) AS max_index
        FROM tblMonitoringTransportCosts AS mtc
        JOIN tblPeriods AS period ON period.ID_tblPeriod = mtc.FK_tblMonitoringTransportCosts_tblPeriods;
    """,
    #
    "select_monitoring_transport_costs_count_less_index_number": """--sql
        SELECT COUNT(*) AS number
        FROM tblMonitoringTransportCosts AS mtc
        JOIN tblPeriods AS period ON period.ID_tblPeriod = mtc.FK_tblMonitoringTransportCosts_tblPeriods
        WHERE period.index_num IS NOT NULL AND period.index_num > 0 AND period.index_num < ?;
    """,
    #
    "delete_monitoring_transport_costs_less_max_idex": """--sql
        DELETE FROM tblMonitoringTransportCosts
        WHERE ID_MonitoringTransportCost IN (
            SELECT mtc.ID_MonitoringTransportCost
            FROM tblMonitoringTransportCosts AS mtc
            JOIN tblPeriods AS period ON period.ID_tblPeriod = mtc.FK_tblMonitoringTransportCosts_tblPeriods
            WHERE
                period.index_num IS NOT NULL
                AND period.index_num > 0
                AND period.index_num < ?
                AND mtc.ID_MonitoringTransportCost IS NOT NULL
                AND period.ID_tblPeriod IS NOT NULL
        );
    """,
    #
    "delete_table_monitoring_transport_costs": """--sql DROP TABLE IF EXISTS tblMonitoringTransportCosts;""",
    "delete_table_history_monitoring_transport_costs": """--sql DROP TABLE IF EXISTS _tblHistoryMonitoringTransportCosts;""",
    "delete_view_monitoring_transport_costs": """--sql DROP VIEW IF EXISTS viewMonitoringTransportCosts;""",
    #
    # --- > Хранение мониторинга цен на Транспортные Услуги
    "create_table_monitoring_transport_costs": """--sql
        CREATE TABLE tblMonitoringTransportCosts
            (
                ID_MonitoringTransportCost                  INTEGER PRIMARY KEY NOT NULL,
                FK_tblMonitoringTransportCosts_tblProducts  INTEGER NOT NULL,
                FK_tblMonitoringTransportCosts_tblPeriods   INTEGER NOT NULL,
                --
                actual_price REAL NOT NULL DEFAULT 0.0,
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                FOREIGN KEY (FK_tblMonitoringTransportCosts_tblProducts) REFERENCES tblProducts (ID_tblProduct),
                FOREIGN KEY (FK_tblMonitoringTransportCosts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
                UNIQUE (FK_tblMonitoringTransportCosts_tblProducts, FK_tblMonitoringTransportCosts_tblPeriods)
            );
        """,
    # -----------------------------------------------------------------------------------
    #
    "create_index_monitoring__transport_costs": """--sql
        CREATE UNIQUE INDEX idxMonitoringTransportCosts ON tblMonitoringTransportCosts (
            FK_tblMonitoringTransportCosts_tblProducts, FK_tblMonitoringTransportCosts_tblPeriods
            );
    """,
    #
    "insert_monitoring_transport_costs": """--sql
        INSERT INTO tblMonitoringTransportCosts (
            FK_tblMonitoringTransportCosts_tblProducts,
            FK_tblMonitoringTransportCosts_tblPeriods,
            actual_price
        )
        VALUES (?, ?, ?);
    """,
    #
    "update_monitoring_transport_costs_by_id": """--sql
            UPDATE tblMonitoringTransportCosts
            SET
                FK_tblMonitoringTransportCosts_tblProducts = ?,
                FK_tblMonitoringTransportCosts_tblPeriods = ?,
                actual_price = ?
            WHERE ID_MonitoringTransportCost = ?;
        """,
    #
    "create_view_monitoring_transport_costs": """--sql
        CREATE VIEW viewMonitoringTransportCosts AS
            SELECT
                periods.title AS period,
                products.code AS code,
                products.description AS description,
                transport_costs.actual_price AS price
            FROM tblMonitoringTransportCosts AS transport_costs
            INNER JOIN tblPeriods AS periods
                ON periods.ID_tblPeriod = transport_costs.FK_tblMonitoringTransportCosts_tblPeriods
            INNER JOIN tblProducts AS products
                ON products.ID_tblProduct = transport_costs.FK_tblMonitoringTransportCosts_tblProducts
            ORDER BY products.digit_code;
    """,
    #
    # --- > История ----------------------------------------------------------------------
    "create_table_history_monitoring_transport_costs": """--sql
        CREATE TABLE _tblHistoryMonitoringTransportCosts (
            _rowid                                      INTEGER,
            ID_MonitoringTransportCost                  INTEGER,
            FK_tblMonitoringTransportCosts_tblProducts  INTEGER,
            FK_tblMonitoringTransportCosts_tblPeriods   INTEGER,
            actual_price                                REAL,
            last_update                                 INTEGER,
            _version                                    INTEGER NOT NULL,
            _updated                                    INTEGER NOT NULL,
            _mask                                       INTEGER NOT NULL
        );
        """,
    "create_index_history_monitoring_transport_costs": """--sql
        CREATE INDEX idxHistoryMonitoringTransportCosts ON _tblHistoryMonitoringTransportCosts (_rowid);
    """,
    # --- > Триггеры базовой таблицы -----------------------------------------------------
    "create_trigger_history_monitoring_transport_costs_insert": """--sql
        CREATE TRIGGER tgrHistoryMonitoringTransportCostsInsert
        AFTER INSERT ON tblMonitoringTransportCosts
        BEGIN
            INSERT INTO _tblHistoryMonitoringTransportCosts (
                _rowid,
                ID_MonitoringTransportCost,
                FK_tblMonitoringTransportCosts_tblProducts,
                FK_tblMonitoringTransportCosts_tblPeriods,
                actual_price,
                last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid,
                new.ID_MonitoringTransportCost,
                new.FK_tblMonitoringTransportCosts_tblProducts,
                new.FK_tblMonitoringTransportCosts_tblPeriods,
                new.actual_price,
                new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,
    #
    "create_trigger_history_monitoring_transport_costs_delete": """--sql
        CREATE TRIGGER tgrHistoryMonitoringTransportCostsDelete
        AFTER DELETE ON tblMonitoringTransportCosts
        BEGIN
            INSERT INTO _tblHistoryMonitoringTransportCosts (
                _rowid,
                ID_MonitoringTransportCost,
                FK_tblMonitoringTransportCosts_tblProducts,
                FK_tblMonitoringTransportCosts_tblPeriods,
                actual_price,
                last_update,
                _version,
                _updated,
                _mask
            )
            VALUES (
                old.rowid,
                old.ID_MonitoringTransportCost,
                old.FK_tblMonitoringTransportCosts_tblProducts,
                old.FK_tblMonitoringTransportCosts_tblPeriods,
                old.actual_price,
                old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryMonitoringTransportCosts WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                -1
            );
        END;
    """,
    #
    "create_trigger_history_monitoring_transport_costs_update": """--sql
        CREATE TRIGGER tgrHistoryMonitoringTransportCostsUpdate
        AFTER UPDATE ON tblMonitoringTransportCosts
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryMonitoringTransportCosts (
                _rowid,
                ID_MonitoringTransportCost,
                FK_tblMonitoringTransportCosts_tblProducts,
                FK_tblMonitoringTransportCosts_tblPeriods,
                actual_price,
                last_update,
                _version,
                _updated,
                _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_MonitoringTransportCost != new.ID_MonitoringTransportCost THEN new.ID_MonitoringTransportCost ELSE null END,
                CASE WHEN old.FK_tblMonitoringTransportCosts_tblProducts != new.FK_tblMonitoringTransportCosts_tblProducts THEN new.FK_tblMonitoringTransportCosts_tblProducts ELSE null END,
                CASE WHEN old.FK_tblMonitoringTransportCosts_tblPeriods != new.FK_tblMonitoringTransportCosts_tblPeriods THEN new.FK_tblMonitoringTransportCosts_tblPeriods ELSE null END,
                --
                CASE WHEN old.actual_price != new.actual_price THEN new.actual_price ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                --
                (SELECT MAX(_version) FROM _tblHistoryMonitoringTransportCosts WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                (CASE WHEN old.ID_MonitoringTransportCost != new.ID_MonitoringTransportCost then 1 else 0 END) +
                (CASE WHEN old.FK_tblMonitoringTransportCosts_tblProducts != new.FK_tblMonitoringTransportCosts_tblProducts then 2 else 0 END) +
                (CASE WHEN old.FK_tblMonitoringTransportCosts_tblPeriods != new.FK_tblMonitoringTransportCosts_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.actual_price != new.actual_price then 8 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 16 else 0 END)
            WHERE
                old.ID_MonitoringTransportCost != new.ID_MonitoringTransportCost OR
                old.FK_tblMonitoringTransportCosts_tblProducts != new.FK_tblMonitoringTransportCosts_tblProducts OR
                old.FK_tblMonitoringTransportCosts_tblPeriods != new.FK_tblMonitoringTransportCosts_tblPeriods OR
                old.actual_price != new.actual_price OR
                old.last_update != new.last_update;
        END;
    """,
}