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

