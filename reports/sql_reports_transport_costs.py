sql_transport_costs_reports = {
    "select_period_id_for_max_index": """--sql
    -- найти максимальный индекс периода, получить id периода транспортных расходов
        SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
        FROM tblTransportCosts tc
        JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE tc.base_price > 0
        GROUP BY p.ID_tblPeriod;
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
        -- ? id записи для которой строится история
        SELECT
            p.index_num,
            htc._rowid id,
            htc.FK_tblTransportCosts_tblPeriods period_id,
            htc.base_price,
            htc.actual_price
        FROM _tblHistoryTransportCosts htc
        JOIN tblPeriods AS p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
        WHERE
            htc._rowid = ?
            AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
        ORDER BY _version DESC; --ASC
    """,
    #
    "select_id_records_for_max_index": """--sql
        -- получить id записей транспортных расходов для максимального периода и базовая цена != 0
        -- ? id периода
        WITH vars(rwd, max) AS (
            SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
            FROM tblTransportCosts tc
            JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE tc.base_price > 0
            GROUP BY p.ID_tblPeriod
            )
        SELECT pd.code, pd.description, tc.*
        FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        LEFT JOIN tblProducts AS pd ON pd.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
        WHERE per.ID_tblPeriod = (select rwd from vars) AND base_price > 0;
    """,
}
