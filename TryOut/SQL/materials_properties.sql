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
            FOREIGN KEY (FK_tblMaterials_tblProducts) REFERENCES tblProducts (ID_Product),
            FOREIGN KEY (FK_tblMaterials_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
            FOREIGN KEY (FK_tblMaterials_tblTransportCosts) REFERENCES tblTransportCosts (ID_tblTransportCost),
            --
            UNIQUE (FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods)
        );

CREATE UNIQUE INDEX idxMaterials ON tblMaterials (
            FK_tblMaterials_tblProducts, FK_tblMaterials_tblPeriods
        );

select * from tblTransportCosts; 
select * from tblTransportCosts where base_normative_id = 33258806;
        
select * from tblProducts pr
INNER JOIN tblTransportCosts tc on tc.FK_tblTransportCosts_tblProducts = pr.ID_tblProduct
;

select * from tblProducts pr
where pr.code = '1.1-1-5689'
;
/*

period_id=150996873
transport_cost_id=33258806
storage_cost_id=30210511	
pressmark=1.1-1-5685
*/

select * from tblPeriods pr where pr.basic_database_id=150996873;
-- 152
select * from tblProducts pd where pd.code='1.1-1-5685';
select * from _tblHistoryProducts hpd where hpd.code='1.1-1-5685';
-- _rowid 50172
select * from _tblHistoryProducts hpd where hpd._rowid=(select _rowid from _tblHistoryProducts where code='1.1-1-5685');

select ID_tblTransportCost from _tblHistoryTransportCosts where base_normative_id = 33258806;
select * from _tblHistoryTransportCosts where _rowid = 25;

SELECT ID_tblTransportCost
        FROM _tblHistoryTransportCosts
        WHERE base_normative_id = 30210511;


