sql_transport_costs_reports = {
    "select_period_id_for_max_index": """--sql
        -- получить id периода транспортных расходов у которого индекс периода максимальный
        SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
        FROM tblTransportCosts tc
        JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE tc.base_price > 0
        GROUP BY period.ID_tblPeriod;
    """,
    "select_id_records_for_period_id": """--sql
        -- получить id записей транспортных расходов для периода у которых базовая цена != 0
        -- ? id периода
        SELECT FK_tblTransportCosts_tblProducts AS id_tc
        FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE per.ID_tblPeriod = ? AND base_price > 0;
    """,
    #
    "select_historical_prices_for_transport_cost_id": """--sql
        -- получить историю изменения цен транс.расхода по id по периодам
        -- только тогда когда меняется период
        -- ? id записи для которой строится история
        SELECT
            period.index_num,
            htc._rowid id,
            htc.FK_tblTransportCosts_tblPeriods period_id,
            htc.base_price,
            htc.actual_price
        FROM _tblHistoryTransportCosts htc
        JOIN tblPeriods AS period ON period.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
        WHERE
            htc._rowid = ?
            AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
            --AND htc.actual_price IS NOT NULL
        ORDER BY _version ASC; --ASC DESC
    """,
    #
    "select_historical_prices_for_transport_cost_id_not_empty_actual_price": """--sql
        /*
        получить историю изменения цен транс.расхода по id по периодам
        только тогда когда меняется период. Если нет актуальной цены то берем ближайшую цену из истории
        ? id записи для которой строится история
        */
        SELECT
            period.index_num,
            htc._rowid id,
            htc.FK_tblTransportCosts_tblPeriods period_id,
            htc.base_price,
            COALESCE(htc.actual_price, (SELECT actual_price FROM _tblHistoryTransportCosts WHERE
                _rowid = htc._rowid AND _version < htc._version AND actual_price IS NOT NULL ORDER BY _version DESC LIMIT 1
            )) AS actual_price
        FROM _tblHistoryTransportCosts htc
        JOIN tblPeriods AS period ON period.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
        WHERE
            htc._rowid = ?
            AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
        ORDER BY _version ASC;
    """,
    #
    "select_records_for_max_index": """--sql
        -- получить id записей транспортных расходов у которых базовая цена != 0 и индекс периода максимальный
        WITH vars(pid, max) AS (
            SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
            FROM tblTransportCosts tc
            JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE tc.base_price > 0
            GROUP BY period.ID_tblPeriod
            )
        SELECT products.code, products.description, tc.*
        FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        LEFT JOIN tblProducts AS products ON products.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
        WHERE period.ID_tblPeriod = (select pid from vars) AND base_price > 0;
    """,
    #
    "select_records_for_max_index_with_monitoring": """--sql
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
        WHERE period.ID_tblPeriod = (select pid from vars) AND base_price > 0
        ORDER BY products.digit_code DESC;
    """,
}
