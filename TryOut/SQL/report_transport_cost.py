
SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
FROM tblTransportCosts tc
JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
WHERE tc.base_price > 0
GROUP BY period.ID_tblPeriod;

SELECT p.index_num, p.comment FROM tblPeriods p WHERE ID_tblPeriod = 55
;

-- получить id записей транспортных расходов для максимального периода и базовая цена != 0
-- ? id периода
WITH vars(pid, max) AS (
    SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
    FROM tblTransportCosts tc
    JOIN tblPeriods AS period ON period.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
    WHERE tc.base_price > 0
    GROUP BY period.ID_tblPeriod
    )
SELECT 
    period.index_num AS index_num,
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
ORDER BY products.digit_code DESC
;

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
    htc._rowid = 10
    AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
    AND htc.actual_price IS NOT NULL
ORDER BY _version DESC; --ASC



