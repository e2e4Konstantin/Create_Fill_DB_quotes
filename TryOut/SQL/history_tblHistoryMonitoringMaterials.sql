SELECT
    history._rowid,
    (
        SELECT lh.supplier_price
        FROM _tblHistoryMonitoringMaterials lh
        JOIN tblPeriods p ON p.ID_tblPeriod = lh.FK_tblMonitoringMaterial_tblPeriods
        WHERE
            lh._rowid = history._rowid
            AND lh.supplier_price IS NOT NULL
--            AND lh._version <= history._version
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
--            AND lh._version <= history._version
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
    --
    (
        SELECT 
            (
                SELECT lhp.code
                FROM _tblHistoryProducts lhp
                JOIN tblPeriods p ON p.ID_tblPeriod = lhp.FK_tblProducts_tblPeriods
                WHERE lhp._rowid = hprod._rowid
                    AND lhp.code IS NOT NULL            
                    AND p.index_num <= php.index_num
                LIMIT 1
            ) AS lhp_code
        FROM _tblHistoryProducts hprod
        JOIN tblPeriods php ON php.ID_tblPeriod = hprod.FK_tblProducts_tblPeriods
        WHERE 
            php.index_num >= period.index_num AND 
            hprod._rowid =  history.FK_tblMonitoringMaterial_tblProducts --last_products
    ) AS last_code,
    --
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

SELECT 
    (
        SELECT lhp.code
        FROM _tblHistoryProducts lhp
        JOIN tblPeriods p ON p.ID_tblPeriod = lhp.FK_tblProducts_tblPeriods
        WHERE lhp._rowid = hprod._rowid
            AND lhp.code IS NOT NULL            
            AND p.index_num <= php.index_num
        LIMIT 1
    ) AS last_code,
    php.index_num,
    *
FROM _tblHistoryProducts hprod
JOIN tblPeriods php ON php.ID_tblPeriod = hprod.FK_tblProducts_tblPeriods
WHERE 
--    FK_tblProducts_tblPeriods = 16 AND 
    hprod._rowid = 55734
;

--
-- * ---------------------------------------------------------------------------------------
--
WITH
    -- product_code
        latest_product_code AS (
            SELECT hp._rowid, MAX(p.supplement_num) AS latest_period, hp.ID_tblProduct, hp.code, hp.description
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