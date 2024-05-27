

WITH
    -- product_code
    latest_product_code AS (
        SELECT hp.code, hp._rowid, MAX(p.supplement_num) AS latest_period, hp.ID_tblProduct, hp.description
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.code IS NOT NULL
        GROUP BY hp._rowid
    )
SELECT
    history._rowid,
    lpc.code,
    (
        SELECT lh.supplier_price
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE
            lh._rowid = history._rowid
            AND lh.supplier_price IS NOT NULL
            AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_supplier_price,
    (
        SELECT lh.delivery
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE lh._rowid = history._rowid
            AND lh.delivery IS NOT NULL
            AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_delivery,
    (
        SELECT lh.FK_tblMonitoringMaterial_tblProducts
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE lh._rowid = history._rowid
            AND lh.FK_tblMonitoringMaterial_tblProducts IS NOT NULL
            AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_products,
    history._version,
    period.index_num,
    period.supplement_num
FROM _tblHistoryMonitoringMaterials history
JOIN tblPeriods period ON period.ID_tblPeriod = history.FK_tblMonitoringMaterial_tblPeriods
JOIN latest_product_code lpc ON lpc._rowid = last_products --55734
WHERE history._rowid = 2 --11239
ORDER BY history._rowid, history._version
;

