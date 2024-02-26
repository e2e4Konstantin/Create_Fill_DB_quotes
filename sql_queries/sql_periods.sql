DROP TABLE IF EXISTS tblPeriods;

CREATE TABLE tblPeriods (
    ID_tblPeriod    INTEGER PRIMARY KEY NOT NULL,
    FK_Holder_tblItems_tblPeriods   INTEGER NOT NULL,   -- id держателя
    FK_Type_tblItems_tblPeriods     INTEGER NOT NULL,   -- id типа
    ID_parent                       INTEGER REFERENCES tblPeriods (ID_tblPeriod),
    start_date                      TEXT COLLATE NOCASE NOT NULL CHECK(DATE(start_date, '+0 days') == start_date),
    supplement_num                  INTEGER NOT NULL,                   -- номер дополнения
    index_num                       INTEGER NOT NULL,                   -- номер индекса
    title                           TEXT NOT NULL,

    FOREIGN KEY (FK_Type_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),
    FOREIGN KEY (FK_Holder_tblItems_tblPeriods) REFERENCES tblItems (tblItem),

    UNIQUE (supplement_num, title)
);
