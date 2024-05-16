WITH
    -- supplier_price
    latest_supplier_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_supplier_price, supplier_price
        FROM  _tblHistoryMonitoringMaterials
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
        WHERE supplier_price IS NOT NULL and p.index_num <= :index_number
        GROUP BY _rowid
    ),
    -- delivery
    latest_delivery AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_delivery, delivery
        FROM  _tblHistoryMonitoringMaterials
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
        WHERE delivery IS NOT NULL and p.index_num <= :index_number
        GROUP BY _rowid
    ),
    -- title
    latest_title AS (
        SELECT mm1._rowid, MAX(p.index_num) AS latest_period_title, mm1.title
        FROM  _tblHistoryMonitoringMaterials mm1
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
        WHERE mm1.title IS NOT NULL AND p.index_num <= :index_number
        GROUP BY mm1._rowid
    ),
    -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, index_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'мониторинг')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
            AND p.index_num = :index_number
    )
    SELECT
        hmm._rowid AS monitoring_material_rowid,
        tp.index_num AS monitoring_materials_index_number,
        hmm.ID_tblMonitoringMaterial,
        lsp.supplier_price AS monitoring_supplier_price,
        ld.delivery AS monitoring_delivery,
        lt.title AS monitoring_title,
        hmm.FK_tblMonitoringMaterial_tblProducts AS monitoring_product_id
    FROM _tblHistoryMonitoringMaterials hmm
    JOIN target_periods tp ON tp.ID_tblPeriod = hmm.FK_tblMonitoringMaterial_tblPeriods
    JOIN latest_supplier_price lsp ON lsp._rowid = hmm._rowid
    JOIN latest_delivery ld ON ld._rowid = hmm._rowid
    JOIN latest_title lt ON lt._rowid = hmm._rowid
    WHERE hmm.FK_tblMonitoringMaterial_tblProducts IS NOT NULL AND hmm.FK_tblMonitoringMaterial_tblProducts = :rowid
    ;

-- Удаление строк из tblMonitoringMaterials, имеющих соответствующую строку в tblPeriods с index_num
-- больше указанного index_number.
-- Если index_num равен null, сравнение всегда будет возвращать false, поэтому строка не будет удалена.
DELETE FROM tblMonitoringMaterials
WHERE ID_tblMonitoringMaterial IN (
    SELECT ID_tblMonitoringMaterial
    FROM tblMonitoringMaterials
    JOIN tblPeriods AS p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
    WHERE
        ID_tblMonitoringMaterial IS NOT NULL
        AND p.ID_tblPeriod IS NOT NULL
        AND p.index_num IS NOT NULL
        AND p.index_num > 0
        AND p.index_num < :index_number
);

SELECT hm0._rowid, MAX(p.index_num) AS latest_period_index, hm0._mask, 777, hm0.*
            FROM _tblHistoryMaterials hm0
            JOIN tblPeriods p ON p.ID_tblPeriod = hm0.FK_tblMaterials_tblPeriods
            WHERE  p.index_num <= 211
--            and hm0._rowid = 43
            and hm0._rowid NOT IN  (SELECT hm1._rowid FROM _tblHistoryMaterials hm1 WHERE hm1._mask < 0)
            GROUP BY hm0._rowid
            ;


/*
Из истории продуктов для номера дополнения 71 получаем не удаленные записи
и протягиваем последние изменения полей
*/
WITH
    -- FK_tblProducts_tblOrigins
    latest_origins AS (
        SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_origins, hp.FK_tblProducts_tblOrigins
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.FK_tblProducts_tblOrigins IS NOT NULL and p.supplement_num <= 71
        GROUP BY hp._rowid
    ),
    -- FK_tblProducts_tblItems
    latest_item AS (
        SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_item, hp.FK_tblProducts_tblItems
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.FK_tblProducts_tblItems IS NOT NULL AND p.supplement_num <= 71
        GROUP BY hp._rowid
    ),
    -- code
    latest_code AS (
        SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_code, hp.code
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.code IS NOT NULL AND p.supplement_num <= 71
        GROUP BY hp._rowid
    ),
    -- description
    latest_description AS (
        SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_description, hp.description
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.description IS NOT NULL AND p.supplement_num <= 71
        GROUP BY hp._rowid
    ),
    -- measurer
    latest_measurer AS (
        SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_measurer, hp.measurer
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.measurer IS NOT NULL and p.supplement_num <= 71
        GROUP BY hp._rowid
    ),
    -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, p.supplement_num, p.title
        FROM tblPeriods p
        WHERE
            p.FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND p.FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE team = 'periods_category' and name = 'supplement')
            AND p.supplement_num = 71
    )
SELECT
    hp._mask,
    hp._rowid,
    lcode.code,
    ldesc.description,
    lmeas.measurer
FROM _tblHistoryProducts hp
JOIN target_periods tp ON tp.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
JOIN latest_origins lorigin ON lorigin._rowid = hp._rowid
JOIN latest_item litem ON litem._rowid = hp._rowid
JOIN latest_code lcode ON lcode._rowid = hp._rowid
JOIN latest_description ldesc ON ldesc._rowid = hp._rowid
JOIN latest_measurer lmeas ON lmeas._rowid = hp._rowid
WHERE
    hp._mask != -1
    AND lorigin.FK_tblProducts_tblOrigins = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')    --1
    AND litem.FK_tblProducts_tblItems = (SELECT ID_tblItem FROM tblItems WHERE team = 'units' and name = 'material')    --4
;