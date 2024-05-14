WITH 
    -- ID_TransportCost
    latest_ID_tblTransportCost AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index_id, ID_tblTransportCost 
        FROM _tblHistoryTransportCosts 
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblTransportCosts_tblPeriods
        WHERE ID_tblTransportCost IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),
    -- base_price
    latest_base_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index_bp, base_price 
        FROM _tblHistoryTransportCosts 
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblTransportCosts_tblPeriods
        WHERE base_price IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ), 
    -- actual_price
    latest_actual_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_index_bp, actual_price 
        FROM _tblHistoryTransportCosts 
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblTransportCosts_tblPeriods
        WHERE actual_price IS NOT NULL and p.index_num <= 207
        GROUP BY _rowid
    ),    
     -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, index_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
            AND p.index_num = 207
     )       
SELECT htc._rowid, tp.index_num, lidtc.ID_tblTransportCost, lbp.base_price, lap.actual_price 
FROM _tblHistoryTransportCosts htc
JOIN target_periods tp ON tp.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
JOIN latest_ID_tblTransportCost lidtc ON lidtc._rowid = htc._rowid
JOIN latest_base_price lbp ON lbp._rowid = htc._rowid
JOIN latest_actual_price lap ON lap._rowid = htc._rowid
WHERE htc._rowid = 40
;
