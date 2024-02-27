CREATE TABLE tblPeriods (
        ID_tblPeriod                    INTEGER PRIMARY KEY NOT NULL,
        FK_Holder_tblItems_tblPeriods   INTEGER NOT NULL,   -- id держателя/владельца для которых ведутся периоды (ТСН, Оборудование, Мониторинг)
        FK_Type_tblItems_tblPeriods     INTEGER NOT NULL,   -- id типа периода (Дополнение, Индекс)
        ID_parent                       INTEGER REFERENCES tblPeriods (ID_tblPeriod), -- период родителя
        date_start                      TEXT COLLATE NOCASE NOT NULL CHECK(DATE(date_start, '+0 days') == date_start),
        supplement_num                  INTEGER NOT NULL,   -- номер дополнения
        index_num                       INTEGER NOT NULL,   -- номер индекса
        title                           TEXT NOT NULL,      -- название периода
        comment                         TEXT,               -- комментарий к периоду

        FOREIGN KEY (FK_Type_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),
        FOREIGN KEY (FK_Holder_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),

        UNIQUE (supplement_num, index_num, title, FK_Type_tblItems_tblPeriods, FK_Holder_tblItems_tblPeriods)
        );
        
        
        