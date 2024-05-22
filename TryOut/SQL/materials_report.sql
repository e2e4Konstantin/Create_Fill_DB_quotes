WITH
    -- period
    target_periods AS (
        SELECT  MAX(p.index_num) AS max_index, p.ID_tblPeriod, p.index_num, p.supplement_num, p.title
        FROM tblMaterials m
        JOIN tblPeriods p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE m.base_price > 0
        GROUP BY p.ID_tblPeriod
    )
SELECT
    materials.ID_tblMaterial,
    tp.index_num,
    products.code,
    products.description AS title,
    --
    materials.net_weight,
    materials.gross_weight,
    materials.base_price,
    materials.actual_price,
    --
    (
       SELECT ptr.code FROM tblProducts ptr WHERE ptr.ID_tblProduct = COALESCE(tc.FK_tblTransportCosts_tblProducts, -1)
    ) AS transport_code,
    COALESCE(tc.base_price, 0) AS transport_base_price,
    COALESCE(tc.inflation_ratio, 0) AS transport_factor,
    --
    (SELECT index_num FROM tblPeriods WHERE ID_tblPeriod = COALESCE(monitoring.FK_tblMonitoringMaterial_tblPeriods, -1)) AS monitoring_index_num,
    COALESCE(monitoring.supplier_price, 0) AS monitoring_price,
    --monitoring.FK_tblMonitoringMaterial_tblPeriods AS monitoring_period_id,
    COALESCE(monitoring.delivery, 0) AS transport_flag
FROM tblMaterials materials
JOIN target_periods tp ON tp.ID_tblPeriod = materials.FK_tblMaterials_tblPeriods
JOIN tblProducts AS products ON products.ID_tblProduct = materials.FK_tblMaterials_tblProducts
LEFT JOIN tblTransportCosts AS tc ON tc.ID_tblTransportCost = materials.FK_tblMaterials_tblTransportCosts
LEFT JOIN tblMonitoringMaterials AS monitoring ON monitoring.FK_tblMonitoringMaterial_tblProducts = products.ID_tblProduct
WHERE
    materials.base_price > 0
    AND COALESCE(tc.FK_tblTransportCosts_tblPeriods, -1) = tp.ID_tblPeriod
ORDER BY products.digit_code ASC
--LIMIT 10
        ;

  -- получить Материалы у которых базовая цена != 0 и индекс периода максимальный
        WITH vars(period_id, max_period_index) AS (
            SELECT p.ID_tblPeriod, MAX(p.index_num)
            FROM tblMaterials m
            JOIN tblPeriods AS p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
            WHERE m.base_price > 0
            GROUP BY p.ID_tblPeriod
        )
        SELECT
            materials.ID_tblMaterial,
            periods.index_num,
            products.code,
            products.description AS title,
            --
            materials.net_weight,
            materials.gross_weight,
            materials.base_price,
            materials.actual_price,
            monitoring.supplier_price AS monitoring_price,
            monitoring.FK_tblMonitoringMaterial_tblPeriods AS monitoring_period_id,
            (SELECT index_num FROM tblPeriods WHERE ID_tblPeriod = monitoring.FK_tblMonitoringMaterial_tblPeriods) AS monitoring_index_num,
            monitoring.delivery AS transport_flag,
            --
            (
               SELECT ptr.code
               FROM tblProducts AS ptr
               WHERE ptr.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
            ) AS transport_code,
            COALESCE(tc.base_price, 0) AS transport_base_price,
            COALESCE(tc.inflation_ratio, 0) AS transport_factor

        FROM tblMaterials materials
        JOIN tblPeriods AS periods ON periods.ID_tblPeriod = materials.FK_tblMaterials_tblPeriods
        JOIN tblProducts AS products ON products.ID_tblProduct = materials.FK_tblMaterials_tblProducts
        LEFT JOIN tblMonitoringMaterials AS monitoring ON monitoring.FK_tblMonitoringMaterial_tblProducts = products.ID_tblProduct
        LEFT JOIN tblTransportCosts AS tc ON tc.ID_tblTransportCost = materials.FK_tblMaterials_tblTransportCosts
        JOIN vars ON vars.period_id = periods.ID_tblPeriod
        WHERE periods.index_num = vars.max_period_index AND materials.base_price > 0
        ORDER BY products.digit_code ASC
        --LIMIT 10
        ;