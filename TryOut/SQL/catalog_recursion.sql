SELECT p.id, p.pressmark, p.parent FROM tblRawData AS p;

SELECT (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent AS INT) ) AS [parent_pressmark], m.*
FROM tblRawData AS m
order by m.pressmark_sort
limit 20;

-- ('periods_category', 'supplement')
SELECT i.ID_tblItem FROM tblItems AS i WHERE i.team = 'periods_category' AND  i.name = 'supplement';

SELECT i.* FROM tblItems AS i WHERE i.team = 'periods_category' AND i.name = 'supplement';



SELECT MAX(m.FK_tblCatalogs_tblPeriods) AS max_period
        FROM tblCatalogs m
        WHERE m.ID_tblCatalog IN (
            WITH CatalogLevel AS (
                SELECT ID_tblCatalog, ID_parent from tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?
                UNION ALL
                SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
            )
            SELECT ID_tblCatalog from CatalogLevel
        );

-- все записи для которых родителем является запись с code

-- SELECT MAX(m.FK_tblCatalogs_tblPeriods) AS max_period
   --     FROM tblCatalogs m

WITH CatalogLevel AS (
    SELECT ID_tblCatalog, ID_parent
        FROM tblCatalogs
        WHERE FK_tblCatalogs_tblOrigins = 1 AND code = '4'
    UNION ALL
    SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
    JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
)
SELECT ID_tblCatalog from CatalogLevel;
 

select * from tblCatalogs where code REGEXP "^4";

SELECT MAX(per.supplement_num) AS max_supplement
FROM tblCatalogs AS cat
JOIN tblPeriods AS per ON per.ID_tblPeriod = cat.FK_tblCatalogs_tblPeriods
WHERE cat.ID_tblCatalog IN (  
4, 62, 63, 64, 65, 66, 67, 68, 69, 70);


SELECT MAX(per.supplement_num) AS max_supplement
FROM tblCatalogs AS cat
JOIN tblPeriods AS per ON per.ID_tblPeriod = cat.FK_tblCatalogs_tblPeriods
WHERE cat.ID_tblCatalog IN (  
    WITH CatalogLevel AS (
    SELECT ID_tblCatalog, ID_parent
        FROM tblCatalogs
        WHERE FK_tblCatalogs_tblOrigins = 1 AND code = '4'
    UNION ALL
    SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
    JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
    )
    SELECT ID_tblCatalog from CatalogLevel
);


SELECT COUNT(cat.ID_tblCatalog) AS count
FROM tblCatalogs AS cat
JOIN tblPeriods AS per ON per.ID_tblPeriod = cat.FK_tblCatalogs_tblPeriods
WHERE cat.ID_tblCatalog IN (
        --
        WITH CatalogLevel AS (
            SELECT ID_tblCatalog, ID_parent
            FROM tblCatalogs
            WHERE FK_tblCatalogs_tblOrigins = 1 AND code = '4'
            UNION ALL
                SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
        )
        SELECT ID_tblCatalog from CatalogLevel
        --            
      ) AND
      per.supplement_num <= 67;

/*
SELECT COUNT(m.FK_tblCatalogs_tblPeriods) AS count
    FROM tblCatalogs m
    WHERE m.ID_tblCatalog IN (
            WITH CatalogLevel AS (
                SELECT ID_tblCatalog, ID_parent from tblCatalogs WHERE  FK_tblCatalogs_tblOrigins = ? AND code = ?
                UNION ALL
                SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
                )
            SELECT ID_tblCatalog from CatalogLevel
        )
        AND m.period < ?;

*/







        