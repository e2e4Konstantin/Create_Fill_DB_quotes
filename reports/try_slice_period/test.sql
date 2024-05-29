

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

SELECT
    resource.pressmark AS "шифр",
    resource.title AS "нименование",
    unit_of_measure.title AS "ед.изм",
    resource.price AS "базовая",
    resource.current_price AS "текущая",
    resource.current_sale_price AS "Текущая отпускная",
    resource.inflation_rate AS "индекс",
    ROUND(resource.price * resource.inflation_rate, 2) AS calc
FROM larix.resources resource
JOIN larix.period period ON period.id = resource.period_id
JOIN larix.unit_of_measure unit_of_measure ON unit_of_measure.id = resource.unit_of_measure_id
WHERE resource.deleted = 0
  AND period.id = 167403321
  AND resource.pressmark IN (
    '1.1-1-2613',
    '1.1-1-2558',
    '1.1-1-1762',
    '1.1-1-3508',
    '1.1-1-2947',
    '1.26-2-17',
    '1.1-1-2566',
    '1.1-1-1051',
    '1.1-1-3002'
  )
ORDER BY resource.pressmark_sort;


