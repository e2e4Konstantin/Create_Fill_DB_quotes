sql_monitoring_materials = {
    "select_monitoring_materials_by_product_and_period": """--sql
        SELECT * FROM tblMonitoringMaterials WHERE FK_tblMonitoringMaterial_tblProducts = ? AND FK_tblMonitoringMaterial_tblPeriods = ?;
    """,
    "select_monitoring_materials_by_product": """--sql
        SELECT * FROM tblMonitoringMaterials WHERE FK_tblMonitoringMaterial_tblProducts = ?;
    """,
    #
    "select_monitoring_materials_max_index_number": """--sql
        SELECT MAX(period.index_num) AS max_index
        FROM tblMonitoringMaterials AS mm
        JOIN tblPeriods AS period ON period.ID_tblPeriod = mm.FK_tblMonitoringMaterial_tblPeriods;
    """,
    #
    "select_monitoring_materials_count_less_index_number": """--sql
        SELECT COUNT(*) AS number
        FROM tblMonitoringMaterials AS mm
        JOIN tblPeriods AS period ON period.ID_tblPeriod = mm.FK_tblMonitoringMaterial_tblPeriods
        WHERE
            period.index_num IS NOT NULL
            AND period.index_num > 0
            AND period.index_num < ?;
    """,
    #
    "delete_monitoring_materials_less_max_idex": """--sql
        -- Удаление строк из tblMonitoringMaterials,
        -- имеющих соответствующую строку в tblPeriods с index_num
        -- больше указанного index_number.
        -- Если index_num равен null, сравнение всегда будет возвращать false,
        -- поэтому строка не будет удалена.
        DELETE FROM tblMonitoringMaterials
        WHERE ID_tblMonitoringMaterial IN (
            SELECT ID_tblMonitoringMaterial
            FROM tblMonitoringMaterials
            JOIN tblPeriods AS p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
            WHERE
                ID_tblMonitoringMaterial IS NOT NULL
                AND p.ID_tblPeriod IS NOT NULL
                AND p.index_num IS NOT NULL
                AND p.index_num > 0
                AND p.index_num < :index_number
        );
    """,
    # -----------------------------------------------------------------------------------
    #
    "delete_table_monitoring_materials": """--sql DROP TABLE IF EXISTS tblMonitoringMaterials;""",
    "delete_table_history_monitoring_materials": """--sql DROP TABLE IF EXISTS _tblHistoryMonitoringMaterials;""",
    "delete_view_monitoring_materials": """--sql DROP VIEW IF EXISTS viewMonitoringMaterials;""",
    #
    "insert_monitoring_materials": """--sql
        INSERT INTO tblMonitoringMaterials (
            FK_tblMonitoringMaterial_tblProducts, FK_tblMonitoringMaterial_tblPeriods,
            supplier_price, delivery, title
        )
        VALUES (?, ?, ?, ?, ?);
    """,
    #
    "update_monitoring_materials_by_id": """--sql
            UPDATE tblMonitoringMaterials
            SET
                FK_tblMonitoringMaterial_tblProducts = ?,
                FK_tblMonitoringMaterial_tblPeriods = ?,
                supplier_price = ?, delivery = ?, title = ?
            WHERE ID_tblMonitoringMaterial = ?;
        """,
    #
    # --- > Базовая таблица для хранения мониторинга цен поставщиков на материалы
    "create_table_monitoring_materials": """--sql
        CREATE TABLE tblMonitoringMaterials
            (
                ID_tblMonitoringMaterial              INTEGER PRIMARY KEY NOT NULL,
                FK_tblMonitoringMaterial_tblProducts  INTEGER NOT NULL,
                FK_tblMonitoringMaterial_tblPeriods   INTEGER NOT NULL,
                --
                supplier_price  REAL NOT NULL DEFAULT 0.0,
                delivery        INTEGER NOT NULL DEFAULT 0,
                title           TEXT,
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                FOREIGN KEY (FK_tblMonitoringMaterial_tblProducts) REFERENCES tblProducts (ID_tblProduct),
                FOREIGN KEY (FK_tblMonitoringMaterial_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
                UNIQUE (FK_tblMonitoringMaterial_tblProducts, FK_tblMonitoringMaterial_tblPeriods)
            );
        """,
    "create_index_monitoring_materials": """--sql
        CREATE UNIQUE INDEX idxMonitoringMaterials ON tblMonitoringMaterials (
            FK_tblMonitoringMaterial_tblProducts, FK_tblMonitoringMaterial_tblPeriods
            );
    """,
    #
    "create_view_monitoring_materials": """--sql
        CREATE VIEW viewMonitoringMaterials AS
            SELECT
                per.title AS period,
                prd.code AS code,
                mm.supplier_price AS supplier_price,
                mm.delivery AS delivery,
                mm.title AS title
            FROM tblMonitoringMaterials mm
            LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = mm.FK_tblMonitoringMaterial_tblPeriods
            LEFT JOIN tblProducts AS prd ON prd.ID_tblProduct = mm.FK_tblMonitoringMaterial_tblProducts
            ORDER BY prd.digit_code;

    """,
    # --- > История ----------------------------------------------------------------------
    "create_table_history_monitoring_materials": """--sql
        CREATE TABLE _tblHistoryMonitoringMaterials (
            _rowid                               INTEGER,
            ID_tblMonitoringMaterial             INTEGER,
            FK_tblMonitoringMaterial_tblProducts INTEGER,
            FK_tblMonitoringMaterial_tblPeriods  INTEGER,
            supplier_price                       REAL,
            delivery                             INTEGER,
            title                                TEXT,
            last_update   INTEGER,
            _version      INTEGER NOT NULL,
            _updated      INTEGER NOT NULL,
            _mask         INTEGER NOT NULL
        );
        """,
    "create_index_history_monitoring_materials": """--sql
        CREATE INDEX idxHistoryMonitoringMaterials ON _tblHistoryMonitoringMaterials (_rowid);
    """,
    # --- > Триггеры базовой таблицы -----------------------------------------------------
    "create_trigger_history_monitoring_materials_insert": """--sql
        CREATE TRIGGER tgrHistoryMonitoringMaterialsInsert
        AFTER INSERT ON tblMonitoringMaterials
        BEGIN
            INSERT INTO _tblHistoryMonitoringMaterials (
                _rowid,
                ID_tblMonitoringMaterial,
                FK_tblMonitoringMaterial_tblProducts,
                FK_tblMonitoringMaterial_tblPeriods,
                supplier_price, delivery, title,
                last_update, _version, _updated, _mask
            )
            VALUES (
                new.rowid,
                new.ID_tblMonitoringMaterial,
                new.FK_tblMonitoringMaterial_tblProducts,
                new.FK_tblMonitoringMaterial_tblPeriods,
                new.supplier_price, new.delivery, new.title,
                new.last_update, 1, unixepoch('now'), 0
            );
        END;
    """,
    "create_trigger_history_monitoring_materials_delete": """--sql
        CREATE TRIGGER tgrHistoryMonitoringMaterialsDelete
        AFTER DELETE ON tblMonitoringMaterials
        BEGIN
            INSERT INTO _tblHistoryMonitoringMaterials (
                _rowid,
                ID_tblMonitoringMaterial,
                FK_tblMonitoringMaterial_tblProducts,
                FK_tblMonitoringMaterial_tblPeriods,
                supplier_price, delivery, title,
                last_update,
                _version,
                _updated,
                _mask
            )
            VALUES (
                old.rowid,
                old.ID_tblMonitoringMaterial,
                old.FK_tblMonitoringMaterial_tblProducts,
                old.FK_tblMonitoringMaterial_tblPeriods,
                old.supplier_price, old.delivery, old.title,
                old.last_update,
                (
                    SELECT COALESCE(MAX(_version), 0)
                    FROM _tblHistoryMonitoringMaterials
                    WHERE _rowid = old.rowid
                ) + 1,
                unixepoch('now'),
                -1
            );
        END;
    """,
    #
    "create_trigger_history_monitoring_materials_update": """--sql
        CREATE TRIGGER tgrHistoryMonitoringMaterialsUpdate
        AFTER UPDATE ON tblMonitoringMaterials
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryMonitoringMaterials (
                _rowid,
                ID_tblMonitoringMaterial,
                FK_tblMonitoringMaterial_tblProducts,
                FK_tblMonitoringMaterial_tblPeriods,
                supplier_price, delivery, title,
                last_update,
                _version,
                _updated,
                _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblMonitoringMaterial != new.ID_tblMonitoringMaterial THEN new.ID_tblMonitoringMaterial ELSE null END,
                CASE WHEN old.FK_tblMonitoringMaterial_tblProducts != new.FK_tblMonitoringMaterial_tblProducts THEN new.FK_tblMonitoringMaterial_tblProducts ELSE null END,
                CASE WHEN old.FK_tblMonitoringMaterial_tblPeriods != new.FK_tblMonitoringMaterial_tblPeriods THEN new.FK_tblMonitoringMaterial_tblPeriods ELSE null END,
                --
                CASE WHEN old.supplier_price != new.supplier_price THEN new.supplier_price ELSE null END,
                CASE WHEN old.delivery != new.delivery THEN new.delivery ELSE null END,
                CASE WHEN old.title != new.title THEN new.title ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                --
                (SELECT MAX(_version) FROM _tblHistoryMonitoringMaterials WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                --
                (CASE WHEN old.ID_tblMonitoringMaterial != new.ID_tblMonitoringMaterial then 1 else 0 END) +
                (CASE WHEN old.FK_tblMonitoringMaterial_tblProducts != new.FK_tblMonitoringMaterial_tblProducts then 2 else 0 END) +
                (CASE WHEN old.FK_tblMonitoringMaterial_tblPeriods != new.FK_tblMonitoringMaterial_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.supplier_price != new.supplier_price then 8 else 0 END) +
                (CASE WHEN old.delivery != new.delivery then 16 else 0 END) +
                (CASE WHEN old.title != new.title then 32 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 64 else 0 END)
            WHERE
                old.ID_tblMonitoringMaterial != new.ID_tblMonitoringMaterial OR
                old.FK_tblMonitoringMaterial_tblProducts != new.FK_tblMonitoringMaterial_tblProducts OR
                old.FK_tblMonitoringMaterial_tblPeriods != new.FK_tblMonitoringMaterial_tblPeriods OR
                old.supplier_price != new.supplier_price OR
                old.delivery != new.delivery OR
                old.title != new.title OR
                old.last_update != new.last_update;
        END;
    """,
}
