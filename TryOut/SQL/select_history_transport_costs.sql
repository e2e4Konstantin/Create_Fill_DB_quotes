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




