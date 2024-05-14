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

CREATE UNIQUE INDEX idxMonitoringMaterials ON tblMonitoringMaterials (
    FK_tblMonitoringMaterial_tblProducts, FK_tblMonitoringMaterial_tblPeriods
            );

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

CREATE INDEX idxHistoryMonitoringMaterials ON _tblHistoryMonitoringMaterials (_rowid);


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


WITH latest_actual_price AS (
    SELECT _rowid, MAX(p.index_num) AS latest_period_index, actual_price
    FROM _tblHistoryMaterials hm
    JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
    WHERE hm.actual_price IS NOT NULL and p.index_num <= 207
    GROUP BY _rowid
),
--
latest_base_price AS (
    SELECT _rowid, MAX(p.index_num) AS latest_period_index, base_price
    FROM _tblHistoryMaterials hm
    JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
    WHERE hm.base_price IS NOT NULL and p.index_num <= 207
    GROUP BY _rowid
),
--
latest_estimate_price AS (
    SELECT _rowid, MAX(p.index_num) AS latest_period_index, estimate_price
    FROM _tblHistoryMaterials hm
    JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
    WHERE hm.estimate_price IS NOT NULL and p.index_num <= 207
    GROUP BY _rowid
),
--
target_periods AS (
    SELECT p.ID_tblPeriod, index_num
    FROM tblPeriods p
    WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
        AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
        AND p.index_num = 207
)
SELECT hm._rowid, tp.index_num, lap.actual_price, lep.estimate_price, lbp.base_price
FROM _tblHistoryMaterials hm
JOIN target_periods tp ON tp.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
JOIN latest_actual_price lap ON lap._rowid = hm._rowid
JOIN latest_estimate_price lep ON lep._rowid = hm._rowid
JOIN latest_base_price lbp ON lbp._rowid = hm._rowid
WHERE hm._rowid = 6;
