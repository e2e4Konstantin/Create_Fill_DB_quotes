SELECT
    history._rowid,
    (
        SELECT lh.supplier_price
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE
            lh._rowid = history._rowid
            AND lh.supplier_price IS NOT NULL
            AND lh._version <= history._version
            --AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_supplier_price,
    (
        SELECT lh.delivery
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE lh._rowid = history._rowid
            AND lh.delivery IS NOT NULL
            AND lh._version <= history._version
--            AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_delivery,
    history._version,
    history.delivery,
    history.supplier_price,
    period.index_num,
    period.title as period_title,
    history.*
FROM _tblHistoryMonitoringMaterials history
JOIN tblPeriods period ON period.ID_tblPeriod = history.FK_tblMonitoringMaterial_tblPeriods
WHERE history._rowid = 11239
ORDER BY history._rowid, history._version
;

WITH
    -- product_code
        latest_product_code AS (
            SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_supplement, hp.ID_tblProduct, hp.code, hp.description
            FROM _tblHistoryProducts hp
            JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
            WHERE hp.code IS NOT NULL AND p.supplement_num <= period.supplement_num
            GROUP BY hp._rowid
        )
SELECT
    history._rowid,
    (
        SELECT lh.supplier_price
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE
            lh._rowid = history._rowid
            AND lh.supplier_price IS NOT NULL
            AND lh._version <= history._version
            --AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_supplier_price,
    (
        SELECT lh.delivery
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE lh._rowid = history._rowid
            AND lh.delivery IS NOT NULL
            AND lh._version <= history._version
--            AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_delivery,
    history._version,
    history.delivery,
    history.supplier_price,
    period.index_num,
    period.title as period_title,
    history.*
FROM _tblHistoryMonitoringMaterials history
JOIN tblPeriods period ON period.ID_tblPeriod = history.FK_tblMonitoringMaterial_tblPeriods
JOIN latest_product_code lpc ON lpc._rowid = history.FK_tblMaterials_tblProducts
WHERE history._rowid = 11239
ORDER BY history._rowid, history._version
;        