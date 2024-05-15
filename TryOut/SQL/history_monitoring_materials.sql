WITH 
    -- supplier_price
    latest_supplier_price AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_supplier_price, supplier_price
        FROM  _tblHistoryMonitoringMaterials 
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods 
        WHERE supplier_price IS NOT NULL and p.index_num <= 208
        GROUP BY _rowid
    ),
    -- delivery
    latest_delivery AS (
        SELECT _rowid, MAX(p.index_num) AS latest_period_delivery, delivery
        FROM  _tblHistoryMonitoringMaterials 
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods 
        WHERE delivery IS NOT NULL and p.index_num <= 208
        GROUP BY _rowid
    ),
    -- title 
    latest_title AS (
        SELECT mm1._rowid, MAX(p.index_num) AS latest_period_title, mm1.title
        FROM  _tblHistoryMonitoringMaterials mm1
        JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods 
        WHERE mm1.title IS NOT NULL AND p.index_num <= 208
        GROUP BY mm1._rowid
    ),
     -- period
    target_periods AS (
        SELECT p.ID_tblPeriod, index_num
        FROM tblPeriods p
        WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'мониторинг')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
            AND p.index_num = 208
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
--WHERE hmm.FK_tblMonitoringMaterial_tblProducts = 44547
;

