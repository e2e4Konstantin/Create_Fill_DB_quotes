

sql_materials = {
    #
    # FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods, FK_tblMaterials_tblTransportCosts, FK_tblMaterials_tblStorageCosts,
    # description, RPC, RPCA2, net_weight, gross_weight, used_to_calc_avg_rate, base_price, actual_price, estimate_price
    #
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
        UPDATE tblTransportCosts
        SET
            FK_tblMaterials_tblProducts = ?, FK_tblMaterials_tblPeriods = ?,
            FK_tblMaterials_tblTransportCosts = ?, FK_tblMaterials_tblStorageCosts = ?,
            description = ?, RPC = ?, RPCA2 = ?, net_weight = ?, gross_weight = ?,
            used_to_calc_avg_rate = ?, base_price = ?, actual_price = ?, estimate_price = ?
        WHERE ID_tblMaterial = ?;
    """,
    # ----------------------------------------------------------------------
    "delete_table_materials": """DROP TABLE IF EXISTS tblMaterials;""",
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
}
