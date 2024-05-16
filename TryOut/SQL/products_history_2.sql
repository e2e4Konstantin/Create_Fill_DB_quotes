WITH
        -- ID_tblMaterial
        latest_ID_tblMaterial AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, ID_tblMaterial
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.ID_tblMaterial IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- actual_price
        latest_actual_price AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, actual_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.actual_price IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- base_price
        latest_base_price AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, base_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.base_price IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- estimate_price
        latest_estimate_price AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, estimate_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.estimate_price IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- net_weight
        latest_net_weight AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, net_weight
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.net_weight IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- gross_weight
        latest_gross_weight AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, gross_weight
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.gross_weight IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
         -- FK_tblMaterials_tblProducts
        latest_FK_tblMaterials_tblProducts AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, FK_tblMaterials_tblProducts
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.FK_tblMaterials_tblProducts IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- FK_tblMaterials_tblTransportCosts
        latest_FK_tblMaterials_tblTransportCosts AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_index, FK_tblMaterials_tblTransportCosts
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.FK_tblMaterials_tblTransportCosts IS NOT NULL and p.index_num <= 210
            GROUP BY _rowid
        ),
        -- _mask
        latest_mask AS (
            SELECT hm0._rowid, MAX(p.index_num) AS latest_period_index, hm0._mask
            FROM _tblHistoryMaterials hm0
            JOIN tblPeriods p ON p.ID_tblPeriod = hm0.FK_tblMaterials_tblPeriods
            WHERE hm0._mask > 0 and p.index_num <= 210
            GROUP BY hm0._rowid
        ),
        -- period
        target_periods AS (
            SELECT p.ID_tblPeriod, index_num, supplement_num
            FROM tblPeriods p
            WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
                AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
                AND p.index_num = 210
        )
    SELECT
        hm._mask,
        tp.ID_tblPeriod,
        hm._rowid AS material_rowid, lidm.ID_tblMaterial,
        tp.index_num AS index_number, tp.supplement_num AS index_supplement_number,
        lap.actual_price, lep.estimate_price, lbp.base_price,
        lnw.net_weight, lgw.gross_weight,
        lmp.FK_tblMaterials_tblProducts, lmtc.FK_tblMaterials_tblTransportCosts
    FROM _tblHistoryMaterials hm
    --
    JOIN target_periods tp ON tp.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
    JOIN latest_ID_tblMaterial lidm ON lidm._rowid = hm._rowid
    JOIN latest_actual_price lap ON lap._rowid = hm._rowid
    JOIN latest_estimate_price lep ON lep._rowid = hm._rowid
    JOIN latest_base_price lbp ON lbp._rowid = hm._rowid
    JOIN latest_net_weight lnw ON lnw._rowid = hm._rowid
    JOIN latest_gross_weight lgw ON lgw._rowid = hm._rowid
    JOIN latest_FK_tblMaterials_tblProducts lmp ON lmp._rowid = hm._rowid
    JOIN latest_FK_tblMaterials_tblTransportCosts lmtc ON lmtc._rowid = hm._rowid
    WHERE hm._rowid NOT IN  (SELECT del_hm._rowid FROM _tblHistoryMaterials del_hm WHERE del_hm._mask < 0 )
    -- WHERE hm._rowid = :rowid
    ;
    
SELECT hm0.*
FROM _tblHistoryMaterials hm0
WHERE hm0._rowid = 43
;

SELECT hm1._rowid, hm1._mask 
FROM _tblHistoryMaterials hm1 
WHERE hm1._mask < 0 and FK_tblMaterials_tblPeriods = 68
;

SELECT hm0._rowid, hm0._mask, 777, hm0.*
FROM _tblHistoryMaterials hm0
WHERE  
    hm0.FK_tblMaterials_tblPeriods = 68
    and hm0._rowid NOT IN  (SELECT hm1._rowid FROM _tblHistoryMaterials hm1 WHERE hm1._mask < 0) 
;


SELECT hm0._rowid, MAX(p.index_num) AS latest_period_index, hm0._mask, 777, hm0.*
FROM _tblHistoryMaterials hm0
JOIN tblPeriods p ON p.ID_tblPeriod = hm0.FK_tblMaterials_tblPeriods
WHERE  p.index_num = 210
and hm0._rowid NOT IN  (SELECT hm1._rowid FROM _tblHistoryMaterials hm1 WHERE hm1._mask < 0) 
GROUP BY hm0._rowid;

----------------------------------------------------------------------------------------------
WITH
    -- _mask
    latest_mask AS (
        SELECT hp._rowid, MAX(p.index_num) AS latest_period_index, hp.code
        FROM _tblHistoryProducts hp
        JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        WHERE hp.code IS NOT NULL and p.supplement_num <= 71
        GROUP BY hm0._rowid
    )
select * from _tblHistoryProducts 
where 
    FK_tblProducts_tblPeriods = 16
    --and _rowid NOT IN (SELECT hp1._rowid FROM _tblHistoryProducts hp1 WHERE hp1._mask < 0) 
--    and code  REGEXP '^1.\d+-\d+-\d+'
--    and _mask < 0
;

select * from _tblHistoryProducts ;

select * from _tblHistoryProducts 
where 
    FK_tblProducts_tblPeriods = 16;



SELECT p.supplement_num, hp.*
FROM _tblHistoryProducts hp
JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
WHERE 
    p.supplement_num <= 72
;

WITH
    -- FK_tblProducts_tblOrigins
    latest_origins AS (
        SELECT hp0._rowid, MAX(p.supplement_num) AS latest_period_origins, hp0.FK_tblProducts_tblOrigins
        FROM _tblHistoryProducts hp0
        JOIN tblPeriods p ON p.ID_tblPeriod = hp0.FK_tblProducts_tblPeriods
        WHERE hp0.FK_tblProducts_tblOrigins IS NOT NULL and p.supplement_num <= 72
        GROUP BY hp0._rowid
    ),
    -- FK_tblProducts_tblItems
    latest_item AS (
        SELECT hp0._rowid, MAX(p.supplement_num) AS latest_period_item, hp0.FK_tblProducts_tblItems
        FROM _tblHistoryProducts hp0
        JOIN tblPeriods p ON p.ID_tblPeriod = hp0.FK_tblProducts_tblPeriods
        WHERE hp0.FK_tblProducts_tblItems IS NOT NULL and p.supplement_num <= 72
        GROUP BY hp0._rowid
    ),
    -- code
    latest_code AS (
        SELECT hp1._rowid, MAX(p.supplement_num) AS latest_period_code, hp1.code
        FROM _tblHistoryProducts hp1
        JOIN tblPeriods p ON p.ID_tblPeriod = hp1.FK_tblProducts_tblPeriods
        WHERE hp1.code IS NOT NULL and p.supplement_num <= 72
        GROUP BY hp1._rowid
    ), 
    -- description
    latest_description AS (
        SELECT hp1._rowid, MAX(p.supplement_num) AS latest_period_description, hp1.description
        FROM _tblHistoryProducts hp1
        JOIN tblPeriods p ON p.ID_tblPeriod = hp1.FK_tblProducts_tblPeriods
        WHERE hp1.description IS NOT NULL and p.supplement_num <= 72
        GROUP BY hp1._rowid
    ),
    -- measurer
    latest_measurer AS (
        SELECT hp1._rowid, MAX(p.supplement_num) AS latest_period_measurer, hp1.measurer
        FROM _tblHistoryProducts hp1
        JOIN tblPeriods p ON p.ID_tblPeriod = hp1.FK_tblProducts_tblPeriods
        WHERE hp1.measurer IS NOT NULL and p.supplement_num <= 72
        GROUP BY hp1._rowid
    ),
    -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, supplement_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'supplement')
            AND p.supplement_num = 72
    )    

SELECT hp._rowid,  lcode.code, litem.FK_tblProducts_tblItems, lorigin.FK_tblProducts_tblOrigins, ldesc.description, lmeas.measurer, hp.*
FROM _tblHistoryProducts hp
JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods

JOIN latest_origins lorigin ON lorigin._rowid = hp._rowid
JOIN latest_item litem ON litem._rowid = hp._rowid
JOIN latest_code lcode ON lcode._rowid = hp._rowid
JOIN latest_description ldesc ON ldesc._rowid = hp._rowid
JOIN latest_measurer lmeas ON lmeas._rowid = hp._rowid

WHERE  
    p.supplement_num <= 72 
    and lorigin.FK_tblProducts_tblOrigins = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')    --1
    and litem.FK_tblProducts_tblItems = (SELECT ID_tblItem FROM tblItems WHERE team = 'units' and name = 'material')    --4
--    and lcode.code REGEXP '^1.\d+.+'
    and hp._rowid NOT IN (SELECT hp2._rowid FROM _tblHistoryProducts hp2 WHERE hp2._mask < 0) 
     
;

SELECT ID_tblItem FROM tblItems --WHERE team = 'periods_category' --and name = 'supplement'
;



SELECT *, p.ID_tblPeriod, p.supplement_num
FROM tblPeriods p
WHERE 
    p.FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
    AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'supplement')
--    AND p.supplement_num = 72
    ;





WITH
     -- FK_tblMaterials_tblProducts
    latest_FK_tblMaterials_tblProducts AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index, FK_tblMaterials_tblProducts
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE hm.FK_tblMaterials_tblProducts IS NOT NULL and p.index_num <= 210
        GROUP BY _rowid
    ),
    -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, index_num, supplement_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
            AND p.index_num = 210
    )
SELECT 
--    count(*),
    hm0._rowid,  
    hm0._mask, 
    (lmp.FK_tblMaterials_tblProducts) as product_id,
--    , (select code from _tblHistoryProducts where ID_tblProduct = lmp.FK_tblMaterials_tblProducts limit 1) as product_code
    hm0.*
FROM _tblHistoryMaterials hm0
JOIN target_periods tp ON tp.ID_tblPeriod = hm0.FK_tblMaterials_tblPeriods
JOIN latest_FK_tblMaterials_tblProducts lmp ON lmp._rowid = hm0._rowid
WHERE hm0._rowid NOT IN (SELECT hm1._rowid FROM _tblHistoryMaterials hm1 WHERE hm1._mask < 0) 

;

select code from _tblHistoryProducts where ID_tblProduct = 44545;
select code, title from tblRawData;


SELECT
    per.title AS [period],
    o.name AS origin,
    i.title AS product_type,

    m.code AS code,
    m.description AS title,
    m.measurer AS measurer
FROM tblProducts m

LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = m.FK_tblProducts_tblOrigins
LEFT JOIN tblItems AS i ON i.ID_tblItem = m.FK_tblProducts_tblItems
LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
where m.code  REGEXP '^1.\d+-\d+-\d+'      --1.12-5-750
ORDER BY m.digit_code


