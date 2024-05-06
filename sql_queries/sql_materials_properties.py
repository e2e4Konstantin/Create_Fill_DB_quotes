

sql_materials = {
    #
    # FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods, FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
    # description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate, base_price, actual_price, estimate_price
    #
    # --------------------------------------------------------------------------------------------------------
    "create_view_materials": """--sql
    CREATE VIEW viewMaterials AS
        SELECT
            per.title AS 'период',
            pr.code AS 'шифр',
            m.RPC AS 'ОКП',
            m.RPCA2  AS 'ОКПД2',
            pr.description AS 'название',
            pr.measurer AS 'ед. измер.',
            --
            m.net_weight AS 'нетто',
            m.gross_weight AS 'брутто',
            m.base_price AS 'баз. цена',
            m.actual_price AS 'отп.тек. цена',                        -- ОТЦ: Отпускная Текущая Цена (от мониторинга)
            m.estimate_price AS 'смет.тек. цена',                     -- СТЦ: Сметная Текущая Цена (расчетная)
            m.inflation_ratio AS 'коэф. инфл',                        -- инфляция estimate_price/base_price
            m.calc_estimate_price AS 'смет.обрат.цена',              -- обратный пересчет СТЦ (estimate_price/base_price)*base_price
            --
            (SELECT ptr.code FROM tblProducts AS ptr WHERE ptr.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts) AS 'код транспортировки',
            COALESCE(tc.base_price, 0) AS "Транс.Баз. цена",
            COALESCE(tc.actual_price, 0) AS "Транс.Тек. цена",
            COALESCE(tc.inflation_ratio, 0) AS "Транс.Коэф. Инф"
        FROM tblMaterials m
        LEFT JOIN tblProducts AS pr ON pr.ID_tblProduct = m.FK_tblMaterials_tblProducts
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        LEFT JOIN tblTransportCosts AS tc ON  tc.ID_tblTransportCost = m.FK_tblMaterials_tblTransportCosts
        ORDER BY m.FK_tblMaterials_tblPeriods, pr.digit_code;
    """,
    "delete_materials_less_max_idex": """--sql
        DELETE FROM tblMaterials
        WHERE ID_tblMaterial IN (
            SELECT m.ID_tblMaterial
            FROM tblMaterials AS m
            JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
            WHERE
                per.index_num IS NOT NULL
                AND per.index_num > 0
                AND per.index_num < ?
                AND m.ID_tblMaterial IS NOT NULL
                AND per.ID_tblPeriod IS NOT NULL
        );
    """,
    "select_materials_count_less_index_number": """--sql
        SELECT COUNT(*) AS number
        FROM tblMaterials AS m
        JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE per.index_num IS NOT NULL AND per.index_num > 0 AND per.index_num < ?;
    """,
    "select_materials_max_index_number": """--sql
        SELECT MAX(per.index_num) AS max_index
        FROM tblMaterials AS m
        JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblMaterials_tblPeriods;
    """,
    "select_material_by_product_id": """--sql
        SELECT p.index_num AS index_num, m.*
        FROM tblMaterials m
        JOIN tblPeriods p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE m.FK_tblMaterials_tblProducts = ?;
    """,
    "insert_material": """--sql
        INSERT INTO tblMaterials (
            FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods, FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
            description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate, base_price, actual_price, estimate_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    "update_material_by_id": """--sql
        UPDATE tblMaterials
        SET
            FK_tblMaterials_tblProducts = ?, FK_tblMaterials_tblPeriods = ?,
            FK_tblMaterials_tblTransportCosts = ?, FK_tblMaterials_tblStorageCosts = ?,
            description = ?, RPC = ?, RPCA2 = ?, net_weight = ?, gross_weight = ?,
            used_to_calc_avg_rate = ?, base_price = ?, actual_price = ?, estimate_price = ?
        WHERE ID_tblMaterial = ?;
    """,
    # ----------------------------------------------------------------------
    "delete_table_materials": """DROP TABLE IF EXISTS tblMaterials;""",
    "delete_table_history_materials": """DROP TABLE IF EXISTS _tblHistoryMaterials;""",
    "delete_view_materials": """DROP VIEW IF EXISTS viewMaterials;""",
    "create_table_materials": """--sql
        /*
            Таблица свойств Материалов Глава 1. Без Раздела 0 (Транспортные расходы)
            ID_tblMaterial,
            FK_tblMaterials_tblProducts         -- id продукта
            FK_tblMaterials_tblPeriods          -- id периода
            FK_tblMaterials_tblTransportCosts   -- id транспортной затраты
            FK_tblMaterials_tblStorageCosts     -- id %ЗСР

            description                         -- описание
            RPC                                 -- ОКП (Russian Product Classifier)
            RPCA2                               -- ОКПД2 (RPC by Types of Economic Activities)
            net_weight                          -- вес нетто
            gross_weight                        -- вес брутто
            used_to_calc_avg_rate               -- флаг что материал участвует в расчете среднего индекса

            base_price                          -- СБЦ: Сметная Базисная Цена (на 2000 год)
            actual_price                        -- ОТЦ: Отпускная Текущая Цена (от мониторинга)
            estimate_price                      -- СТЦ: Сметная Текущая Цена (расчетная)
            inflation_ratio                     -- инфляция estimate_price/base_price
            calc_estimate_price                 -- обратный пересчет СТЦ (estimate_price/base_price)*base_price
        */
        CREATE TABLE tblMaterials
        (
            ID_tblMaterial                      INTEGER PRIMARY KEY NOT NULL,
            FK_tblMaterials_tblProducts         INTEGER NOT NULL,
            FK_tblMaterials_tblPeriods          INTEGER NOT NULL,
            FK_tblMaterials_tblTransportCosts   INTEGER,
            FK_tblMaterials_tblStorageCosts     INTEGER,
            description                         TEXT,
            RPC                                 TEXT,
            RPCA2                               TEXT,
            net_weight                          REAL NOT NULL DEFAULT 0.0 CHECK(net_weight >= 0.0),
            gross_weight                        REAL NOT NULL DEFAULT 0.0 CHECK(net_weight >= 0.0),
            used_to_calc_avg_rate               BOOLEAN NOT NULL CHECK (used_to_calc_avg_rate IN (0, 1)) DEFAULT 0,
            --
            base_price                          REAL NOT NULL DEFAULT 0.0 CHECK (base_price >= 0.0),
            actual_price                        REAL NOT NULL DEFAULT 0.0 CHECK (actual_price >= 0.0),
            estimate_price                      REAL NOT NULL DEFAULT 0.0 CHECK (estimate_price >= 0.0),
            inflation_ratio                     REAL GENERATED ALWAYS AS (
                    CASE WHEN base_price = 0 THEN 0 ELSE ROUND(estimate_price / base_price, 2) END
                ) VIRTUAL,
            calc_estimate_price                 REAL GENERATED ALWAYS AS (
                    CASE WHEN base_price = 0 THEN 0 ELSE ROUND(ROUND(estimate_price / base_price, 2) * base_price, 2) END
                ) VIRTUAL,
            --
            last_update     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')) CHECK (last_update >= 0),
            --
            FOREIGN KEY (FK_tblMaterials_tblProducts) REFERENCES tblProducts (ID_tblProduct),
            FOREIGN KEY (FK_tblMaterials_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
            FOREIGN KEY (FK_tblMaterials_tblTransportCosts) REFERENCES tblTransportCosts (ID_tblTransportCost),
            --
            UNIQUE (FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods)
        );
    """,
    "create_index_materials": """--sql
        CREATE UNIQUE INDEX idxMaterials ON tblMaterials (
            FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods
        );
    """,
    # ---------------------------------------------------------------------------------------------
    "create_table_history_materials": """--sql
        -- таблица для хранения историиТранспортных расходов
        CREATE TABLE _tblHistoryMaterials (
            _rowid                           INTEGER,
            ID_tblMaterial                    INTEGER,
            FK_tblMaterials_tblProducts       INTEGER,
            FK_tblMaterials_tblPeriods        INTEGER,
            FK_tblMaterials_tblTransportCosts INTEGER,
            FK_tblMaterials_tblStorageCosts   INTEGER,
            description                       TEXT,
            RPC                               TEXT,
            RPCA2                             TEXT,
            net_weight                        REAL,
            gross_weight                      REAL,
            used_to_calc_avg_rate             INTEGER,
            base_price                        REAL,
            actual_price                      REAL,
            estimate_price                    REAL,
            last_update                       INTEGER,
            _version                          INTEGER NOT NULL,
            _updated                          INTEGER NOT NULL,
            _mask                             INTEGER NOT NULL
        );
        """,
    "create_index_history_materials": """--sql
        CREATE INDEX idxHistoryMaterials ON _tblHistoryMaterials (_rowid);
    """,
    "create_trigger_insert_materials": """--sql
        CREATE TRIGGER tgrHistoryMaterialsInsert
        AFTER INSERT ON tblMaterials
        BEGIN
            INSERT INTO _tblHistoryMaterials (
                _rowid, ID_tblMaterial,
                FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods,
                FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
                description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate,
                base_price, actual_price, estimate_price, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblMaterial,
                new.FK_tblMaterials_tblProducts, new.FK_tblMaterials_tblPeriods,
                new.FK_tblMaterials_tblTransportCosts, new.FK_tblMaterials_tblStorageCosts,
                new.description, new.RPC, new.RPCA2, new.net_weight, new.gross_weight, new.used_to_calc_avg_rate,
                new.base_price, new.actual_price, new.estimate_price, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,
    "create_trigger_delete_materials": """--sql
        CREATE TRIGGER tgrHistoryMaterialsDelete
        AFTER DELETE ON tblMaterials
        BEGIN
            INSERT INTO _tblHistoryMaterials (
                _rowid, ID_tblMaterial,
                FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods,
                FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
                description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate,
                base_price, actual_price, estimate_price, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblMaterial,
                old.FK_tblMaterials_tblProducts, old.FK_tblMaterials_tblPeriods,
                old.FK_tblMaterials_tblTransportCosts, old.FK_tblMaterials_tblStorageCosts,
                old.description, old.RPC, old.RPCA2, old.net_weight, old.gross_weight, old.used_to_calc_avg_rate,
                old.base_price, old.actual_price, old.estimate_price, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryMaterials WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'), -1
            );
        END;
    """,
    "create_trigger_update_materials": """--sql
        CREATE TRIGGER tgrHistoryMaterialsUpdate
        AFTER UPDATE ON tblMaterials
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryMaterials (
                _rowid, ID_tblMaterial,
                FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods,
                FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
                description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate,
                base_price, actual_price, estimate_price, last_update,
                _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblMaterial != new.ID_tblMaterial THEN new.ID_tblMaterial ELSE null END,
                CASE WHEN old.FK_tblMaterials_tblProducts != new.FK_tblMaterials_tblProducts THEN new.FK_tblMaterials_tblProducts ELSE null END,
                CASE WHEN old.FK_tblMaterials_tblPeriods != new.FK_tblMaterials_tblPeriods THEN new.FK_tblMaterials_tblPeriods ELSE null END,
                CASE WHEN old.FK_tblMaterials_tblTransportCosts != new.FK_tblMaterials_tblTransportCosts THEN new.FK_tblMaterials_tblTransportCosts ELSE null END,
                CASE WHEN old.FK_tblMaterials_tblStorageCosts != new.FK_tblMaterials_tblStorageCosts THEN new.FK_tblMaterials_tblStorageCosts ELSE null END,
                --
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.RPC != new.RPC THEN new.RPC ELSE null END,
                CASE WHEN old.RPCA2 != new.RPCA2 THEN new.RPCA2 ELSE null END,
                --
                CASE WHEN old.net_weight != new.net_weight THEN new.net_weight ELSE null END,
                CASE WHEN old.gross_weight != new.gross_weight THEN new.gross_weight ELSE null END,
                CASE WHEN old.used_to_calc_avg_rate != new.used_to_calc_avg_rate THEN new.used_to_calc_avg_rate ELSE null END,
                --
                CASE WHEN old.base_price != new.base_price THEN new.base_price ELSE null END,
                CASE WHEN old.actual_price != new.actual_price THEN new.actual_price ELSE null END,
                CASE WHEN old.estimate_price != new.estimate_price THEN new.estimate_price ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                --
                (SELECT MAX(_version) FROM _tblHistoryMaterials WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'),
                (CASE WHEN old.ID_tblMaterial != new.ID_tblMaterial then 1 else 0 END) +
                (CASE WHEN old.FK_tblMaterials_tblProducts != new.FK_tblMaterials_tblProducts then 2 else 0 END) +
                (CASE WHEN old.FK_tblMaterials_tblPeriods != new.FK_tblMaterials_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.FK_tblMaterials_tblTransportCosts != new.FK_tblMaterials_tblTransportCosts then 8 else 0 END) +
                (CASE WHEN old.FK_tblMaterials_tblStorageCosts != new.FK_tblMaterials_tblStorageCosts then 16 else 0 END) +
                --
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.RPC != new.RPC then 64 else 0 END) +
                (CASE WHEN old.RPCA2 != new.RPCA2 then 128 else 0 END) +
                --
                (CASE WHEN old.net_weight != new.net_weight then 256 else 0 END) +
                (CASE WHEN old.gross_weight != new.gross_weight then 512 else 0 END) +
                (CASE WHEN old.used_to_calc_avg_rate != new.used_to_calc_avg_rate then 1024 else 0 END) +
                --
                (CASE WHEN old.base_price != new.base_price then 2048 else 0 END) +
                (CASE WHEN old.actual_price != new.actual_price then 4096 else 0 END) +
                (CASE WHEN old.estimate_price != new.estimate_price then 8192 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 16384 else 0 END)
            WHERE
                old.ID_tblMaterial != new.ID_tblMaterial OR
                old.FK_tblMaterials_tblProducts != new.FK_tblMaterials_tblProducts OR
                old.FK_tblMaterials_tblPeriods != new.FK_tblMaterials_tblPeriods OR
                old.FK_tblMaterials_tblTransportCosts != new.FK_tblMaterials_tblTransportCosts OR
                old.FK_tblMaterials_tblStorageCosts != new.FK_tblMaterials_tblStorageCosts OR

                old.description != new.description OR
                old.RPC != new.RPC OR
                old.RPCA2 != new.RPCA2 OR
                old.net_weight != new.net_weight OR
                old.gross_weight != new.gross_weight OR
                old.used_to_calc_avg_rate != new.used_to_calc_avg_rate OR
                old.base_price != new.base_price OR
                old.actual_price != new.actual_price OR
                old.estimate_price != new.estimate_price OR
                old.last_update != new.last_update;
        END;
    """,
}
