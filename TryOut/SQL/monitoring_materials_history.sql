/*delete from tblMonitoringMaterials;
delete from _tblHistoryMonitoringMaterials;*/

    WITH
        -- ID_tblMonitoringMaterial
        latest_id_monitoring_material AS (
            SELECT lmm._rowid, MAX(p.index_num) AS latest_period_index, lmm.ID_tblMonitoringMaterial
            FROM  _tblHistoryMonitoringMaterials lmm
            JOIN tblPeriods p ON p.ID_tblPeriod = lmm.FK_tblMonitoringMaterial_tblPeriods
            WHERE lmm.ID_tblMonitoringMaterial IS NOT NULL and p.index_num <= 211
            GROUP BY lmm._rowid
        ),
        -- FK_tblMonitoringMaterial_tblProducts
        latest_FK_tblMonitoringMaterial_tblProducts AS (
            SELECT lmm._rowid, MAX(p.index_num) AS latest_period_index, lmm.FK_tblMonitoringMaterial_tblProducts
            FROM  _tblHistoryMonitoringMaterials lmm
            JOIN tblPeriods p ON p.ID_tblPeriod = lmm.FK_tblMonitoringMaterial_tblPeriods
            WHERE lmm.FK_tblMonitoringMaterial_tblProducts IS NOT NULL and p.index_num <= 211
            GROUP BY lmm._rowid
        ),
        -- supplier_price
        latest_supplier_price AS (
            SELECT lmm._rowid, MAX(p.index_num) AS latest_period_index, lmm.supplier_price
            FROM  _tblHistoryMonitoringMaterials lmm
            JOIN tblPeriods p ON p.ID_tblPeriod = lmm.FK_tblMonitoringMaterial_tblPeriods
            WHERE lmm.supplier_price IS NOT NULL and p.index_num <= 211
            GROUP BY lmm._rowid
        ),
        -- delivery
        latest_delivery AS (
            SELECT lmm._rowid, MAX(p.index_num) AS latest_period_index, lmm.delivery
            FROM  _tblHistoryMonitoringMaterials lmm
            JOIN tblPeriods p ON p.ID_tblPeriod = lmm.FK_tblMonitoringMaterial_tblPeriods
            WHERE lmm.delivery IS NOT NULL and p.index_num <= 211
            GROUP BY lmm._rowid
        ),
        
        -- title
        -- если везде null, то весь запрос работать не будет
        latest_title AS (
            SELECT lmm._rowid, MAX(p.index_num) AS latest_period_index, lmm.title
            FROM  _tblHistoryMonitoringMaterials lmm
            JOIN tblPeriods p ON p.ID_tblPeriod = lmm.FK_tblMonitoringMaterial_tblPeriods
            WHERE lmm.title IS NOT NULL AND p.index_num <= 211
            GROUP BY lmm._rowid
        ),
        -- product_code
            latest_product_code AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_supplement, hp.ID_tblProduct, hp.code, hp.description, hp.digit_code
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.code IS NOT NULL AND p.supplement_num <= (
                    SELECT p.supplement_num AS supplement_number
                    FROM tblPeriods p
                    JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                    JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                    WHERE
                        o.name = 'мониторинг'
                        AND i.team = 'periods_category'
                        AND i.name = 'index'
                        AND p.index_num = 211
                    LIMIT 1 
                    )    --72
                GROUP BY hp._rowid
            ),
        
        -- period
        target_periods AS (
                SELECT p.ID_tblPeriod, p.index_num, p.supplement_num, p.title
                FROM tblPeriods p
                JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                WHERE
                    p.index_num = 211
                    AND o.name  = 'мониторинг'
                    AND i.team = 'periods_category'
                    AND i.name = 'index'
        )
    SELECT
        hmm._rowid AS monitoring_material_rowid,
        lid.ID_tblMonitoringMaterial AS monitoring_material_id,
        tp.ID_tblPeriod AS monitoring_materials_period_id,
        
        --tp.index_num AS monitoring_materials_index_number,

        lsp.supplier_price AS monitoring_supplier_price,
        ld.delivery AS monitoring_delivery,
        lt.title AS monitoring_materials_title,
        lmmp.FK_tblMonitoringMaterial_tblProducts AS monitoring_product_id,
        lpc.code AS monitoring_product_code,
        lpc.digit_code AS monitoring_digit_code
    FROM _tblHistoryMonitoringMaterials hmm
    JOIN latest_id_monitoring_material lid ON lid._rowid = hmm._rowid
    JOIN target_periods tp ON tp.ID_tblPeriod = hmm.FK_tblMonitoringMaterial_tblPeriods
    JOIN latest_FK_tblMonitoringMaterial_tblProducts lmmp ON lmmp._rowid = hmm._rowid
    JOIN latest_supplier_price lsp ON lsp._rowid = hmm._rowid
    JOIN latest_delivery ld ON ld._rowid = hmm._rowid
    JOIN latest_title lt ON lt._rowid = hmm._rowid
    JOIN latest_product_code lpc ON lpc._rowid = lmmp.FK_tblMonitoringMaterial_tblProducts
    ORDER BY lpc.digit_code
;


-- получить номер дополнения для индекса в справочнике ТСН
        SELECT p.supplement_num AS supplement_number
        FROM tblPeriods p
        JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
        JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
        WHERE
            o.name = 'мониторинг'
            AND i.team = 'periods_category'
            AND i.name = 'index'
            AND p.index_num = 210
        LIMIT 1    
        ;

SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_supplement, hp.ID_tblProduct, hp.code, hp.description
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.code IS NOT NULL AND p.supplement_num <= (
                    SELECT p.supplement_num AS supplement_number
                    FROM tblPeriods p
                    JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                    JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                    WHERE
                        o.name = 'мониторинг'
                        AND i.team = 'periods_category'
                        AND i.name = 'index'
                        AND p.index_num = 210
                    LIMIT 1
                    )    --72
                GROUP BY hp._rowid;

