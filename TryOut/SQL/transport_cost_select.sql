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
select FK_tblTransportCosts_tblProducts AS id_tc from tblTransportCosts tc
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

SELECT FK_tblTransportCosts_tblProducts
        FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE per.ID_tblPeriod = 167 AND base_price > 0;

SELECT MAX(p.index_num) max, p.index_num, tc.* 
FROM tblTransportCosts tc
JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
GROUP BY FK_tblTransportCosts_tblPeriods
;

-- найти максимальный индекс периода
SELECT p.ID_tblPeriod, MAX(p.index_num)
FROM tblTransportCosts tc
JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
WHERE tc.base_price > 0
GROUP BY p.ID_tblPeriod;

-- ID_tblPeriod = 167
SELECT tc.* FROM tblTransportCosts tc WHERE tc.base_price > 0 AND FK_tblTransportCosts_tblPeriods = 167;

-- (29, 30, 31)


SELECT 
    p.index_num,
    htc._rowid id,
    htc.FK_tblTransportCosts_tblPeriods period_id,
    htc.base_price,
    htc.actual_price
FROM _tblHistoryTransportCosts htc
JOIN tblPeriods AS p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
WHERE 
    htc._rowid = 29 
    AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
ORDER BY _version ASC;

;

SELECT tc.* FROM tblTransportCosts tc WHERE tc.base_price > 0 limit 5;
select htc._rowid id, htc.* from _tblHistoryTransportCosts htc where htc._rowid=29;

SELECT t.*, period_id, base_price, actual_price
FROM tblTransportCosts t
right join (select htc._rowid id, htc.FK_tblTransportCosts_tblPeriods period_id, htc.base_price base_price, htc.actual_price actual_price 
      from _tblHistoryTransportCosts htc
      where htc._rowid=29
      ) on id = t.ID_tblTransportCost
;

SELECT htc._rowid AS id, htc.FK_tblTransportCosts_tblPeriods AS period_id, htc.base_price, htc.actual_price
FROM _tblHistoryTransportCosts htc
WHERE htc._rowid > 0 AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
ORDER BY _version ASC;


SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
        FROM tblTransportCosts tc
        JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE tc.base_price > 0
        GROUP BY p.ID_tblPeriod;

SELECT MAX(p.index_num) AS max_index
                FROM tblTransportCosts tc
                JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
                WHERE tc.base_price > 0
                GROUP BY p.ID_tblPeriod;        

WITH var(value) AS (
    SELECT 22
    )    
SELECT * FROM tblPeriods WHERE supplement_num = (SELECT value FROM var);   



WITH vars(rwd) AS (
    SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
        FROM tblTransportCosts tc
        JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE tc.base_price > 0
        GROUP BY p.ID_tblPeriod
    )    
select htc._rowid id, htc.* 
from _tblHistoryTransportCosts htc
JOIN vars on vars.period_id = htc._rowid;

;
select htc._rowid id, htc.* from _tblHistoryTransportCosts htc WHERE htc._rowid = 29;


SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
        FROM tblTransportCosts tc
        JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        WHERE tc.base_price > 0
        GROUP BY p.ID_tblPeriod;
-------------------------------------------------------------------------
WITH vars(rwd, max) AS (
    SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
    FROM tblTransportCosts tc
    JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
    WHERE tc.base_price > 0
    GROUP BY p.ID_tblPeriod
    )    
SELECT tc.* FROM tblTransportCosts tc
LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
WHERE per.ID_tblPeriod = (select rwd from vars) AND base_price > 0
;

WITH vars(rwd, max) AS (
            SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
            FROM tblTransportCosts tc
            JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE tc.base_price > 0
            GROUP BY p.ID_tblPeriod
            )
        SELECT pd.code, tc.* FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        LEFT JOIN tblProducts AS pd ON pd.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
        WHERE per.ID_tblPeriod = (select rwd from vars) AND base_price > 0;

SELECT
            p.index_num,
            htc._rowid id,
            htc.FK_tblTransportCosts_tblPeriods period_id,
            htc.base_price,
            htc.actual_price
        FROM _tblHistoryTransportCosts htc
        JOIN tblPeriods AS p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
        WHERE
            htc._rowid = 5
            AND htc.FK_tblTransportCosts_tblPeriods IS NOT NULL
        ORDER BY _version DESC; --ASC


WITH vars(rwd, max) AS (
            SELECT p.ID_tblPeriod AS period_id, MAX(p.index_num) AS max_index
            FROM tblTransportCosts tc
            JOIN tblPeriods AS p ON p.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            WHERE tc.base_price > 0
            GROUP BY p.ID_tblPeriod
            )
        SELECT pd.code, pd.description, tc.* FROM tblTransportCosts tc
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
        LEFT JOIN tblProducts AS pd ON pd.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
        WHERE per.ID_tblPeriod = (select rwd from vars) AND base_price > 0;



