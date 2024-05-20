  /*
        по номеру индекса периода выбирает материалы из таблицы истории
        с последними изменениями до указанного периода.
        Удаленные материалы не учитываются.
        {'index_number': 207} #'rowid': 6,
    */
    WITH
        -- ID_tblMaterial
        latest_ID_tblMaterial AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.ID_tblMaterial
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.ID_tblMaterial IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- actual_price
        latest_actual_price AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.actual_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.actual_price IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- base_price
        latest_base_price AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.base_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.base_price IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- estimate_price
        latest_estimate_price AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.estimate_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.estimate_price IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- net_weight
        latest_net_weight AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.net_weight
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.net_weight IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- gross_weight
        latest_gross_weight AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.gross_weight
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.gross_weight IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- FK_tblMaterials_tblProducts
        latest_FK_tblMaterials_tblProducts AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.FK_tblMaterials_tblProducts
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.FK_tblMaterials_tblProducts IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
        ),
        -- FK_tblMaterials_tblTransportCosts
        latest_FK_tblMaterials_tblTransportCosts AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.FK_tblMaterials_tblTransportCosts
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.FK_tblMaterials_tblTransportCosts IS NOT NULL and p.index_num <= 210
            GROUP BY hm._rowid
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
        hm._rowid AS material_rowid, lidm.ID_tblMaterial,
        tp.index_num AS index_number, tp.supplement_num AS index_supplement_number,
        lap.actual_price, lep.estimate_price, lbp.base_price,
        lnw.net_weight, lgw.gross_weight,
        lmp.FK_tblMaterials_tblProducts, lmtc.FK_tblMaterials_tblTransportCosts,
        hm._mask
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
    WHERE 
        hm._mask != -1
        AND 
        lmp.FK_tblMaterials_tblProducts = 44542
    ;
    
-- * -----------------------------------------------------------------------------------------

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
        tp.index_num AS transport_cost_index_number,
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
    
    
    WHERE 
        htc._mask != -1
        AND 
        htc._rowid = 13
    ;
    

SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_supplement, hp.code
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.code IS NOT NULL AND p.supplement_num <= 71
                GROUP BY hp._rowid;