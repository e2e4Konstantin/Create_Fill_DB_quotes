sql_materials_reports = {
    "select_period_id_for_max_index": """--sql
        -- получить id периода Материалов у которого индекс периода максимальный
        SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
        FROM tblMaterials m
        JOIN tblPeriods AS period ON period.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE m.base_price > 0
        GROUP BY period.ID_tblPeriod;
    """,
    "select_id_records_for_period_id": """--sql
        -- получить id записей Материалов для периода у которых базовая цена != 0
        -- ? id периода
        SELECT m.FK_tblMaterials_tblProducts AS materials_id
        FROM tblMaterials m
        JOIN tblPeriods AS p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE p.ID_tblPeriod = ? AND m.base_price > 0;
    """,
    #
    "select_historical_prices_for_materials_id_not_empty_actual_price": """--sql
        /*
        получить историю изменения цен Материалов по id.
        только тогда когда меняется период. Если нет актуальной цены то берем ближайшую цену из истории
        ? id записи для которой строится история
        ? кол-во истории
        сортировка от большего к меньшему иначе получим раннюю историю а не последнюю в глубину
        */
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
            hm._rowid = ?
            AND hm.FK_tblMaterials_tblPeriods IS NOT NULL
        ORDER BY hm._version DESC
        LIMIT ?;
    """,
    #
    "select_records_for_max_index_with_monitoring": """--sql
        -- получить записей Материалов у которых базовая цена != 0 и индекс периода максимальный
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
        LIMIT 10;
    """,
}