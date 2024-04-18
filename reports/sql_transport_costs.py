sql_transport_costs_reports = {
    "select_period_id_for_max_index": """--sql
        -- получить id периода транспортных расходов
        -- для максимального индекса у которых базовая цена > 0
        SELECT p.ID_tblPeriod
        FROM tblPeriods p
        WHERE p.index_num = (
            SELECT COALESCE(MAX(p2.index_num), 0)
            FROM tblTransportCosts tc
            JOIN tblPeriods p2 ON p2.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE
                p2.index_num IS NOT NULL
                AND tc.base_price > 0
        );
    """,
    "select_id_records_for_period_id": """--sql
        -- получить id записей транспортных расходов для периода у которых базовая цена != 0
        SELECT FK_tblTransportCosts_tblProducts AS id_tc
        FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE per.ID_tblPeriod = ? AND base_price > 0;
    """,
}
