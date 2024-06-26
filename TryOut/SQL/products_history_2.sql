--EXPLAIN query PLAN
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

SELECT *
FROM _tblHistoryProducts hp
WHERE _mask < 0
    and FK_tblProducts_tblPeriods !=16   
;

SELECT _rowid,  _mask, p.supplement_num, *
        FROM _tblHistoryProducts 
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblProducts_tblPeriods
        WHERE _mask < 0 AND p.supplement_num <71
;

SELECT hp_del._rowid 
        FROM _tblHistoryProducts hp_del 
        JOIN tblPeriods p_del ON p_del.ID_tblPeriod = hp_del.FK_tblProducts_tblPeriods
        WHERE hp_del._mask < 0 AND p_del.supplement_num < 71;


