  SELECT
                per.title AS 'период',
                p.code AS 'шифр',
                p.description AS 'название',
                tc.base_price AS 'базовая цена',
                tc.actual_price AS 'текущая цена',
                tc.inflation_ratio AS 'инфл.коэф',
                tc.numeric_ratio AS 'коэф ?',
                tc.base_normative_id AS 'base_id'
            FROM tblTransportCosts tc
            LEFT JOIN tblProducts AS p ON p.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
            LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            ORDER BY tc.FK_tblTransportCosts_tblPeriods, p.digit_code;

-- текущий период
select max(tc.FK_tblTransportCosts_tblPeriods) from tblTransportCosts tc;
--167
select per.index_num from tblPeriods per where per.ID_tblPeriod = 167;
--211


select * from tblPeriods per where per.FK_Category_tblItems_tblPeriods = 29 and per.index_num = (211)-1;
select * from tblPeriods per where per.FK_Category_tblItems_tblPeriods = 29 and per.index_num IN (210, 209, 208);

            
select tc.* 
from _tblHistoryTransportCosts tc
LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
where tc.FK_tblTransportCosts_tblPeriods = 
;

-- code = '1.0-1-21'
select ID_tblProduct from tblProducts where code='1.0-1-21';
-- 44495
select ID_tblTransportCost from tblTransportCosts where FK_tblTransportCosts_tblProducts = 44495;
-- 6
select per.title, tc.* 
from _tblHistoryTransportCosts tc 
LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
where tc._rowid=6
order by _version desc
limit 5
;


-- ОПРЕДЕЛИТЬ МАКСИМАЛЬНЫЙ ИНДЕКСНЫЙ ПЕРИОД
SELECT COALESCE(MAX(per.index_num), 0) AS max_index
FROM tblTransportCosts AS tc
LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
WHERE per.index_num IS NOT NULL;
-- 211
-- получить id периода
select per.ID_tblPeriod from tblPeriods per where per.index_num = 211;
--167

-- получить id транспортных расходов для максимального индекса у которых базовая цена != 0
select FK_tblTransportCosts_tblProducts from tblTransportCosts tc
LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
where per.ID_tblPeriod = 167 and base_price > 0;
;
--
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








