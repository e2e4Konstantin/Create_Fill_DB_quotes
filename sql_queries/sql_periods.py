#DROP TABLE IF EXISTS tblPeriods;


# ---> Периоды -----------------------------------------------------------------------
sql_periods_queries = {
    "delete_table_periods": """--sql DROP TABLE IF EXISTS tblPeriods;""",
    "delete_index_periods": """--sql DROP INDEX IF EXISTS idxPeriods;""",
    
    "create_table_periods": """--sql
    /*
    Таблица для хранения периодов. 
    Владелец по которому ведется период: ТСН, Оборудование, Мониторинг...
    Тип периода: Дополнение, Индекс...
    Периоды ТСН это весь ТСН кроме 13 главы. 13 глава (оборудование) ведется по своим периодам.
    Периоды мониторинга несут смысловую нагрузку индекса (цены). 
    Периоды мониторинга ни как не связаны с периодами ТСН.
    */
    CREATE TABLE tblPeriods (
        ID_tblPeriod                    INTEGER PRIMARY KEY NOT NULL,
        FK_Holder_tblItems_tblPeriods   INTEGER NOT NULL,   -- id держателя/владельца для которых ведутся периоды (ТСН, Оборудование, Мониторинг)
        FK_Type_tblItems_tblPeriods     INTEGER NOT NULL,   -- id типа периода (Дополнение, Индекс)
        ID_parent                       INTEGER REFERENCES tblPeriods (ID_tblPeriod), -- период родителя
        date_start                      TEXT COLLATE NOCASE NOT NULL CHECK(DATE(start_date, '+0 days') == start_date),
        supplement_num                  INTEGER NOT NULL,   -- номер дополнения
        index_num                       INTEGER NOT NULL,   -- номер индекса
        title                           TEXT NOT NULL,      -- название периода
        comment                         TEXT,               -- комментарий к периоду

        FOREIGN KEY (FK_Type_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),
        FOREIGN KEY (FK_Holder_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),

        UNIQUE (supplement_num, index_num, title, FK_Type_tblItems_tblPeriods, FK_Holder_tblItems_tblPeriods)
    );
    """,

    "create_index_periods": """--sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxPeriods ON tblPeriods (
            FK_Holder_tblItems_tblPeriods, FK_Type_tblItems_tblPeriods, supplement_num, index_num
        );
    """,
}
