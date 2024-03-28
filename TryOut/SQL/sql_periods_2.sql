
    

UPDATE tblProducts
SET
    FK_tblProducts_tblCatalogs = 1860, FK_tblProducts_tblItems = 2, FK_tblProducts_tblOrigins = 1, FK_tblProducts_tblPeriods = 66, code = '3.0-0-1', 
    description = 'Затраты на превышение стоимости электроэнергии, получаемой от передвижных электростанций',
    measurer = '1 кВт.-ч.', digit_code = 300000000100000
WHERE ID_tblProduct = 1;
--

UPDATE tblProducts
SET description = 'Затраты на превышение стоимости электроэнергии, получаемой от передвижных электростанций', measurer = '2 кВт.-ч.'
WHERE ID_tblProduct = 1;

 CREATE INDEX IF NOT EXISTS idxRawCodeTmpMaterials ON tblRawData (gwp_pressmark, pressmark);
 

select count(*) from tblProducts;
select count(*) from _tblHistoryProducts;


EXPLAIN QUERY PLAN 
SELECT
      m.*
     FROM tblProducts m
     WHERE
      m.FK_tblProducts_tblPeriods BETWEEN 70 AND 65
      AND FK_tblProducts_tblItems = 2
     ORDER BY m.digit_code;

CREATE UNIQUE INDEX IF NOT EXISTS idxProductsCode ON tblProducts (
            code, FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins, FK_tblProducts_tblPeriods DESC
            );

-- 1
SELECT o.* FROM tblOrigins AS o WHERE o.name = 'ТСН'; 

--4
SELECT o.* FROM tblOrigins AS o WHERE o.name = 'оборудование'; 

SELECT i.ID_tblItem FROM tblItems AS i WHERE i.team = 'periods_category' AND  i.name = 'supplement'; --27

SELECT ID_tblPeriod, supplement_num
    FROM tblPeriods
    WHERE
        FK_Origin_tblOrigins_tblPeriods = 4 AND
        FK_Category_tblItems_tblPeriods = 28;

-- {"id_origin": ton_origin_id, "id_item": category_supplement_id}


UPDATE tblPeriods SET ID_parent = Null;

UPDATE tblPeriods
    SET ID_parent = p.ID_tblPeriod
    FROM (
        SELECT ID_tblPeriod, supplement_num
        FROM tblPeriods
        WHERE
            FK_Origin_tblOrigins_tblPeriods = 4 AND FK_Category_tblItems_tblPeriods = 27
    ) AS p
    WHERE
        p.supplement_num = tblPeriods.supplement_num - 1 AND
        tblPeriods.FK_Origin_tblOrigins_tblPeriods = 4 AND
        tblPeriods.FK_Category_tblItems_tblPeriods = 27;



-- Для индексов ТСН, устанавливает ID родительской записи на ту, где номер индекса меньше на 1.
-- named style parameter: {"id_origin": 4, "id_item": 28}, 28 - index
UPDATE tblPeriods
SET ID_parent = p.ID_tblPeriod
FROM (
    SELECT ID_tblPeriod, index_num
    FROM tblPeriods
    WHERE
        FK_Origin_tblOrigins_tblPeriods = 4 AND
        FK_Category_tblItems_tblPeriods = 28
) AS p
WHERE
    p.index_num = tblPeriods.index_num - 1 AND
    FK_Origin_tblOrigins_tblPeriods = 4 AND
    FK_Category_tblItems_tblPeriods = 28;

SELECT * 
FROM tblPeriods
WHERE
    FK_Origin_tblOrigins_tblPeriods = 4 AND
    FK_Category_tblItems_tblPeriods = 28;


SELECT max(u.index_num) as max_num, u.supplement_num
    FROM tblPeriods u
    WHERE
        u.FK_Origin_tblOrigins_tblPeriods = 4 AND
        u.FK_Category_tblItems_tblPeriods = 28
    GROUP BY u.supplement_num;

-- Для дополнений ТСН устанавливает номер индекса
-- на максимальный из группы индексов предыдущего дополнения
-- named style parameter:
-- {"id_origin": 1, "id_item_index": 28, "id_item_supplement": 27}
UPDATE tblPeriods
SET index_num = p.max_num
FROM (
    SELECT max(u.index_num) as max_num, u.supplement_num
    FROM tblPeriods u
    WHERE
        u.FK_Origin_tblOrigins_tblPeriods = 4 AND
        u.FK_Category_tblItems_tblPeriods = 28
    GROUP BY u.supplement_num
) AS p
WHERE
    p.supplement_num = tblPeriods.supplement_num - 1 AND
    tblPeriods.FK_Origin_tblOrigins_tblPeriods = 4 AND
    tblPeriods.FK_Category_tblItems_tblPeriods = 27;






