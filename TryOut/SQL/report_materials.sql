
SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
    FROM tblMaterials m
    JOIN tblPeriods AS period ON period.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
    WHERE m.base_price > 0
    GROUP BY period.ID_tblPeriod
;    
SELECT m.FK_tblMaterials_tblProducts AS materials_id
        FROM tblMaterials m
        JOIN tblPeriods AS p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE p.ID_tblPeriod = 69         AND m.base_price > 0;

WITH vars(pid, max) AS (
    SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
    FROM tblMaterials m
    JOIN tblPeriods AS p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
    WHERE m.base_price > 0
    GROUP BY p.ID_tblPeriod
    )
SELECT
    period.index_num AS "index_num",
    products.code,
    products.description AS "title",
    materials.*,
    monitoring.supplier_price AS "monitoring_price",
    monitoring.FK_tblMonitoringMaterial_tblPeriods AS "monitoring_period id",
    
    (SELECT p.index_num FROM tblPeriods p WHERE ID_tblPeriod = monitoring.FK_tblMonitoringMaterial_tblPeriods) AS "monitoring_index"
FROM tblMaterials materials
LEFT JOIN tblPeriods AS period ON period.ID_tblPeriod = materials.FK_tblMaterials_tblPeriods
LEFT JOIN tblProducts AS products ON products.ID_tblProduct = materials.FK_tblMaterials_tblProducts
LEFT JOIN tblMonitoringMaterials AS monitoring ON monitoring.FK_tblMonitoringMaterial_tblProducts = products.ID_tblProduct
WHERE period.ID_tblPeriod = (select pid from vars) AND materials.base_price > 0
ORDER BY products.digit_code ASC;

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
            (SELECT index_num FROM tblPeriods WHERE ID_tblPeriod = monitoring.FK_tblMonitoringMaterial_tblPeriods) AS monitoring_period_index,
            monitoring.delivery AS transport,
            --
            (
               SELECT ptr.code
               FROM tblProducts AS ptr
               WHERE ptr.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
           )
           AS 'код транспортировки',
           COALESCE(tc.base_price, 0) AS 'Транс.Баз. цена'
            --
        FROM tblMaterials materials
        JOIN tblPeriods AS periods ON periods.ID_tblPeriod = materials.FK_tblMaterials_tblPeriods
        JOIN tblProducts AS products ON products.ID_tblProduct = materials.FK_tblMaterials_tblProducts
        LEFT JOIN tblMonitoringMaterials AS monitoring ON monitoring.FK_tblMonitoringMaterial_tblProducts = products.ID_tblProduct
        LEFT JOIN tblTransportCosts AS tc ON tc.ID_tblTransportCost = materials.FK_tblMaterials_tblTransportCosts
        JOIN vars ON vars.period_id = periods.ID_tblPeriod
        WHERE periods.index_num = vars.max_period_index AND materials.base_price > 0
        ORDER BY products.digit_code ASC
        LIMIT 10;




SELECT actual_price 
FROM _tblHistoryMaterials 
WHERE
    _rowid = 3
    AND _version < 10 
    AND actual_price IS NOT NULL 
ORDER BY _version DESC LIMIT 1
;            


/*
        получить историю изменения цен Материалов по id. для периодов.
        только тогда когда меняется период. Если нет актуальной цены то берем ближайшую цену из истории
        ? id записи для которой строится история
        ? кол-во истории
        сортировка от большего к меньшему иначе получим раннюю историю а не последнюю в глубину
        */
        SELECT
            hm._version AS "ver",
            period.index_num,
            hm._rowid id,
            hm.FK_tblMaterials_tblPeriods AS "period_id",
            hm.base_price,
            hm.actual_price AS a_price,
            --/*,
            COALESCE(hm.actual_price, (
                SELECT actual_price 
                FROM _tblHistoryMaterials 
                WHERE
                    _rowid = hm._rowid 
                    AND _version < hm._version 
                    AND actual_price IS NOT NULL 
                ORDER BY _version DESC LIMIT 1
            )) AS actual_price
            --*/
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods AS period ON period.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE
            hm._rowid = 3
           AND hm.FK_tblMaterials_tblPeriods IS NOT NULL
        ORDER BY _version DESC
        
        --LIMIT 5
        ;


SELECT
            hm._version AS "version",
            period.index_num,
            hm._rowid AS id,
            hm.FK_tblMaterials_tblPeriods AS "period_id",
            hm.base_price,
            COALESCE(hm.actual_price, (
                SELECT actual_price
                FROM _tblHistoryMaterials
                WHERE
                    _rowid = hm._rowid
                    AND _version < hm._version
                    AND actual_price IS NOT NULL
                ORDER BY _version DESC
                LIMIT 1
            )) AS actual_price
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods AS period ON period.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE
            hm._rowid = 4
            AND hm.FK_tblMaterials_tblPeriods IS NOT NULL
        ORDER BY hm._version DESC
        LIMIT 10;




       WITH vars(pid, max) AS (
            SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
            FROM tblTransportCosts tc
            JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE tc.base_price > 0
            GROUP BY period.ID_tblPeriod
            )
        SELECT
            period.index_num AS "index_num",
            products.code,
            products.description,
            tc.*,
            monitoring.actual_price AS "monitoring_price",
            monitoring.FK_tblMonitoringTransportCosts_tblPeriods AS "monitoring_period id",
            (SELECT p.index_num FROM tblPeriods p WHERE ID_tblPeriod = monitoring.FK_tblMonitoringTransportCosts_tblPeriods) AS "monitoring_index"
        FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        LEFT JOIN tblProducts AS products ON products.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
        LEFT JOIN tblMonitoringTransportCosts AS monitoring ON monitoring.FK_tblMonitoringTransportCosts_tblProducts = products.ID_tblProduct
        WHERE period.ID_tblPeriod = (select pid from vars) AND tc.base_price > 0
        ORDER BY products.digit_code ASC;









