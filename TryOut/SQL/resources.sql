PRAGMA foreign_keys = OF;
DROP TABLE tblPeriods;

CREATE TABLE tblPeriods (
        ID_tblPeriod                    INTEGER PRIMARY KEY NOT NULL,
        title                           TEXT NOT NULL,      -- название периода
        supplement_num                  INTEGER NOT NULL,   -- номер дополнения
        index_num                       INTEGER NOT NULL,   -- номер индекса
        date_start                      TEXT COLLATE NOCASE NOT NULL CHECK(DATE(date_start, '+0 days') == date_start),
        comment                         TEXT,               -- описание периода
        ID_parent                       INTEGER REFERENCES tblPeriods (ID_tblPeriod), -- родительский период
        FK_Origin_tblOrigins_tblPeriods INTEGER NOT NULL,   -- id источника/владельца для которых ведутся периоды (ТСН, Оборудование, Мониторинг)
        FK_Category_tblItems_tblPeriods INTEGER NOT NULL,   -- id типа периода (Дополнение, Индекс)
        basic_database_id               INTEGER,            -- id основной бд (Postgres Normative)
        last_update                     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
        FOREIGN KEY (FK_Origin_tblOrigins_tblPeriods) REFERENCES tblOrigins (ID_tblOrigin),
        FOREIGN KEY (FK_Category_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),

        UNIQUE (FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, supplement_num, index_num, title)
    );


--('periods_category', 'дополнение')    

SELECT i.ID_tblItem FROM tblItems AS i WHERE i.team = 'periods_category' AND i.name = 'supplement';

-- (1, 27, 69, 72)

SELECT p.ID_tblPeriod AS [id], p.title AS [title], p.basic_database_id AS [basic_id],
            p.supplement_num AS [supplement], p.index_num AS [index]
        FROM tblPeriods AS p
        WHERE
        p.FK_Origin_tblOrigins_tblPeriods = 1 AND
        p.FK_Category_tblItems_tblPeriods = 27 AND
        p.supplement_num >= 69 AND p.supplement_num <= 72        
        ORDER BY p.supplement_num DESC;

DELETE FROM tblCatalogs WHERE code != '0.0' AND FK_tblCatalogs_tblPeriods = 72 and rowid = 3;


SELECT MAX(per.supplement_num) AS max_suppl
        FROM tblProducts AS m
        JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
        WHERE m.FK_tblProducts_tblOrigins = 1 AND m.FK_tblProducts_tblItems = 2;




SELECT
    (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*
FROM tblRawData AS m;

SELECT
    (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*
FROM tblRawData AS m 
WHERE m.pressmark = '1.1-1';


SELECT
    (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*
    FROM tblRawData AS m 
    WHERE m.pressmark REGEXP '(^\s*1\..*)|(^\s*1\s*$)'; --and m.pressmark REGEXP '^\s*(\d+)\s*$'; --'^\s*(\d+)\s*$'; --

SELECT f.* FROM tblRawData f WHERE f.pressmark REGEXP '(^\s*1\..*)|(^\s*1\s*$)';

SELECT
    (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*, m.*
    FROM (SELECT f.* FROM tblRawData f WHERE f.pressmark REGEXP '(^\s*1\..*)|(^\s*1\s*$)') AS m 
    WHERE m.pressmark REGEXP '^\s*(\d+)\s*$'; 

--'^\s*(\d+)\s*$'
-- '^\s*((\d+)\.(\d+))\s*$'

CREATE INDEX IF NOT EXISTS idxRawCodeTmpMaterials ON tblRawData (gwp_pressmark, pressmark);




