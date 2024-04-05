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
--tblPeriods.FK_Origin_tblOrigins_tblPeriods, tblPeriods.FK_Category_tblItems_tblPeriods, tblPeriods.supplement_num, tblPeriods.index_num:


select o.ID_tblOrigin from tblOrigins o where o.name = 'ТСН';
--1

select i.ID_tblItem from tblItems i where i.team = 'periods_category' AND i.name = 'index';
--28

select basic_database_id 
from tblPeriods
where FK_Origin_tblOrigins_tblPeriods = 1 AND FK_Category_tblItems_tblPeriods = 28 AND index_num >= 198
;

select basic_database_id 
from tblPeriods
where 
    FK_Origin_tblOrigins_tblPeriods = (select o.ID_tblOrigin from tblOrigins o where o.name = 'ТСН') AND 
    FK_Category_tblItems_tblPeriods = (select i.ID_tblItem from tblItems i where i.team = 'periods_category' AND i.name = 'index') AND 
    index_num >= 198
;


select ID_tblPeriod from tblPeriods where basic_database_id = 167264731;


















                

