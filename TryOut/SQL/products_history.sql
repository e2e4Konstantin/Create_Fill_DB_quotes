


WITH 
    -- ID_tblMaterial
    latest_ID_tblMaterial AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, ID_tblMaterial
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.ID_tblMaterial IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- actual_price
    latest_actual_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, actual_price
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.actual_price IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- base_price
    latest_base_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, base_price
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.base_price IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- estimate_price
    latest_estimate_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, estimate_price
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.estimate_price IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- net_weight
    latest_net_weight AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, net_weight
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.net_weight IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- gross_weight
    latest_gross_weight AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, gross_weight
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.gross_weight IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, index_num, supplement_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
            AND p.index_num = 207
)
SELECT hm._rowid, lidm.ID_tblMaterial, tp.index_num, lap.actual_price, lep.estimate_price, lbp.base_price, lnw.net_weight, lgw.gross_weight
FROM _tblHistoryMaterials hm


JOIN target_periods tp ON tp.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
JOIN latest_ID_tblMaterial lidm ON lidm._rowid = hm._rowid
JOIN latest_actual_price lap ON lap._rowid = hm._rowid
JOIN latest_estimate_price lep ON lep._rowid = hm._rowid
JOIN latest_base_price lbp ON lbp._rowid = hm._rowid
JOIN latest_net_weight lnw ON lnw._rowid = hm._rowid
JOIN latest_gross_weight lgw ON lgw._rowid = hm._rowid

--WHERE hm._rowid = 6
;
/*
история Продуктов для периода

*/
WITH 
    -- ID_tblProduct 
    latest_id AS (
        SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_id, ID_tblProduct
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.ID_tblProduct IS NOT NULL and p.supplement_num <= 72
        GROUP BY _rowid
    ),
    -- code
    latest_code AS (
        SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_code, code
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.code IS NOT NULL and p.supplement_num <= 72
        GROUP BY _rowid
    ),
    -- description
    latest_description AS (
        SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_description, description
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.description IS NOT NULL and p.supplement_num <= 72
        GROUP BY _rowid
    ),    
    -- measurer
    latest_measurer AS (
        SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_measurer, measurer
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.measurer IS NOT NULL and p.supplement_num <= 72
        GROUP BY _rowid
    ),
    -- digit_code
    latest_digit_code AS (
        SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_digit_code, digit_code
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.digit_code IS NOT NULL and p.supplement_num <= 72
        GROUP BY _rowid
    ),
    -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, supplement_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'supplement')
            AND p.supplement_num = 72
    )
      
SELECT hp._rowid, tp.supplement_num, lid.ID_tblProduct, lc.code, ld.description, lm.measurer, ldc.digit_code
FROM _tblHistoryProducts hp
--
JOIN target_periods tp ON tp.ID_tblPeriod = hp.FK_tblProducts_tblPeriods

JOIN latest_id lid ON lid._rowid = hp._rowid
JOIN latest_code lc ON lc._rowid = hp._rowid
JOIN latest_description ld ON ld._rowid = hp._rowid
JOIN latest_measurer lm ON lm._rowid = hp._rowid
JOIN latest_digit_code ldc ON ldc._rowid = hp._rowid
where hp._rowid = 5917
ORDER BY ldc.digit_code
;

select * from _tblHistoryProducts where _mask >100;
select * from _tblHistoryProducts where _rowid in (5917, 4043);