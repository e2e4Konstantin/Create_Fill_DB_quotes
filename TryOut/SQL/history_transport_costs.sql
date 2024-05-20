WITH
        -- ID_TransportCost
        latest_ID_tblTransportCost AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.ID_tblTransportCost
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.ID_tblTransportCost IS NOT NULL and p.index_num <= 210
            GROUP BY htc._rowid
        ),
        -- base_price
        latest_base_price AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.base_price
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.base_price IS NOT NULL and p.index_num <= 210
            GROUP BY htc._rowid
        ),
        -- actual_price
        latest_actual_price AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.actual_price
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.actual_price IS NOT NULL and p.index_num <= 210
            GROUP BY htc._rowid
        ),
        
    -- FK_tblTransportCosts_tblProducts
        latest_FK_tblTransportCosts_tblProducts AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.FK_tblTransportCosts_tblProducts
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.FK_tblTransportCosts_tblProducts IS NOT NULL and p.index_num <= 210
            GROUP BY htc._rowid
        ),        
        -- product_code
            latest_product_code AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_supplement, hp.code
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.code IS NOT NULL AND p.supplement_num <= 71
                GROUP BY hp._rowid
            ),
        -- period
        target_periods AS (
                SELECT p.ID_tblPeriod, p.index_num, p.supplement_num, p.title
                FROM tblPeriods p
                JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                WHERE
                    p.index_num = 210
                    AND o.name  = 'ТСН'
                    AND i.team = 'periods_category'
                    AND i.name = 'index'
        )
    SELECT
        htc._rowid AS transport_cost_rowid,
        tp.supplement_num AS supplement_num,
        tp.index_num AS index_number,
        lidtc.ID_tblTransportCost,
        lbp.base_price AS transport_cost_base_price,
        lap.actual_price AS transport_cost_actual_price,
        ltcp.FK_tblTransportCosts_tblProducts,
        lpc.code,
        htc._mask
    FROM _tblHistoryTransportCosts htc
    JOIN target_periods tp ON tp.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
    JOIN latest_ID_tblTransportCost lidtc ON lidtc._rowid = htc._rowid
    JOIN latest_base_price lbp ON lbp._rowid = htc._rowid
    JOIN latest_actual_price lap ON lap._rowid = htc._rowid
    JOIN latest_FK_tblTransportCosts_tblProducts ltcp ON ltcp._rowid = htc._rowid
    JOIN latest_product_code lpc ON lpc._rowid = ltcp._rowid
    
    
    /*WHERE 
        htc._mask != -1
        AND 
        htc._rowid = 1*/
    ;