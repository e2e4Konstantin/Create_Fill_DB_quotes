CREATE TABLE tblMaterials
        (
            ID_tblMaterial              INTEGER PRIMARY KEY NOT NULL,
            FK_tblMaterials_tblProducts INTEGER NOT NULL,   -- id продукта
            FK_tblMaterials_tblPeriods  INTEGER NOT NULL,   -- id периода
            FK_tblMaterials_tblTransportCosts INTEGER,      -- id транспортной затраты
            FK_tblMaterials_tblStorageCosts   INTEGER,      -- id %ЗСР
            description                       TEXT,         -- описание
            RPC                               TEXT,         -- ОКП (Russian Product Classifier)
            RPCA2                             TEXT,         -- ОКПД2 (RPC by Types of Economic Activities)
            net_weight                        REAL NOT NULL DEFAULT 0.0 CHECK(net_weight >= 0.0), -- вес нетто
            gross_weight                      REAL NOT NULL DEFAULT 0.0 CHECK(net_weight >= 0.0), -- вес брутто
            used_to_calc_avg_rate             BOOLEAN NOT NULL CHECK (used_to_calc_avg_rate IN (0, 1)) DEFAULT 0,
            --
            base_price                  REAL NOT NULL DEFAULT 0.0 CHECK (base_price >= 0.0),     -- СБЦ: Сметная Базисная Цена (на 2000 год)
            actual_price                REAL NOT NULL DEFAULT 0.0 CHECK (actual_price >= 0.0),   -- ОТЦ: Отпускная Текущая Цена (от мониторинга)
            estimate_price              REAL NOT NULL DEFAULT 0.0 CHECK (estimate_price >= 0.0), -- СТЦ: Сметная Текущая Цена (расчетная)
            inflation_ratio REAL GENERATED ALWAYS AS ( -- инфляция
                CASE WHEN base_price = 0 THEN 0 ELSE ROUND(estimate_price / base_price, 2) END
            ) VIRTUAL,
            calc_estimate_price REAL GENERATED ALWAYS AS ( -- обратный пересчет СТЦ (округленная)
                CASE WHEN base_price = 0 THEN 0 ELSE ROUND(ROUND(estimate_price / base_price, 2) * base_price, 2) END
            ) VIRTUAL,
            --
            last_update     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')) CHECK (last_update >= 0),
            --
            FOREIGN KEY (FK_tblMaterials_tblProducts) REFERENCES tblProducts (ID_Product),
            FOREIGN KEY (FK_tblMaterials_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
            FOREIGN KEY (FK_tblMaterials_tblTransportCosts) REFERENCES tblTransportCosts (ID_tblTransportCost),
            --
            UNIQUE (FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods)
        );
        







    

