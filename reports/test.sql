SELECT lm.delivery, p.index_num
FROM _tblHistoryMonitoringMaterials lm
JOIN tblPeriods p ON p.ID_tblPeriod = lm.FK_tblMonitoringMaterial_tblPeriods
WHERE lm._rowid = 2 AND lm.delivery IS NOT NULL AND p.index_num <= 209
ORDER BY p.index_num DESC
LIMIT 1;



SELECT
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
        WHERE
            lh._rowid = history._rowid
            AND lh.delivery IS NOT NULL
            AND p.index_num <= period.index_num
        ORDER BY p.index_num DESC
        LIMIT 1
    ) AS last_delivery,
    history._version,
    history.delivery,
    history.supplier_price,
    period.index_num,
    period.title
FROM _tblHistoryMonitoringMaterials history
JOIN tblPeriods period ON period.ID_tblPeriod = history.FK_tblMonitoringMaterial_tblPeriods
WHERE history._rowid = 2
ORDER BY history._version
;

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
    period.title
FROM _tblHistoryMonitoringMaterials history
JOIN tblPeriods period ON period.ID_tblPeriod = history.FK_tblMonitoringMaterial_tblPeriods
WHERE history._rowid = 11239
ORDER BY history._rowid, history._version
;
