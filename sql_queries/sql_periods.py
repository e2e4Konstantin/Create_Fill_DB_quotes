#DROP TABLE IF EXISTS tblPeriods;


# ---> Периоды -----------------------------------------------------------------------
sql_periods_queries = {
    "insert_period": """--sql
        INSERT INTO tblPeriods (
            title, supplement_num, index_num, date_start, comment, ID_parent,
            FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,

    "update_periods_supplement_parent": """--sql
        -- Для дополнений ТСН устанавливает ID родительской записи где номер дополнения меньше на 1
        UPDATE tblPeriods
            SET ID_parent = p.ID_tblPeriod
            FROM (SELECT ID_tblPeriod, supplement_num FROM tblPeriods WHERE FK_Origin_tblOrigins_tblPeriods=? AND FK_Category_tblItems_tblPeriods=?) AS p
            WHERE p.supplement_num = tblPeriods.supplement_num - 1;
        """,

        "update_periods_index_parent": """--sql
        -- Для индексов ТСН, устанавливает ID родительской записи на ту, где номер индекса меньше на 1.
        UPDATE tblPeriods
            SET ID_parent = p.ID_tblPeriod
            FROM (select ID_tblPeriod, index_num from tblPeriods where FK_Origin_tblOrigins_tblPeriods=? AND  FK_Category_tblItems_tblPeriods=?) AS p
            WHERE p.index_num = tblPeriods.index_num - 1;
        """,

    "delete_table_periods": """DROP TABLE IF EXISTS tblPeriods;""",
    "delete_index_periods": """DROP INDEX IF EXISTS idxPeriods;""",
    "delete_all_data_periods": """DELETE FROM tblPeriods;""",

    "create_table_periods": """--sql
    /*
    Таблица для хранения периодов.
    Владелец/источник по которому ведется период: ТСН, Оборудование, Мониторинг...
    Категория периода: Дополнение, Индекс...
    Периоды ТСН это весь ТСН кроме 13 главы. 13 глава (оборудование) ведется по своим периодам.
    Периоды мониторинга несут смысловую нагрузку индекса (цены).
    Периоды мониторинга ни как не связаны с периодами ТСН, хотя по смыслу привязаны к индексу.
    */
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
        basic_database_id               INTEGER,            -- id основной бд (по которой ведется основная деятельность)
        last_update                     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
        FOREIGN KEY (FK_Origin_tblOrigins_tblPeriods) REFERENCES tblOrigins (ID_tblOrigin),
        FOREIGN KEY (FK_Category_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),

        UNIQUE (supplement_num, index_num, title, FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods)
    );
    """,

    "create_index_periods": """--sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxPeriods ON tblPeriods (
            FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, supplement_num, index_num
        );
    """,

    "create_view_periods": """--sql
        CREATE VIEW viewPeriods AS
            SELECT
                o.name AS [Раздел],
                i.title AS [Категория],
                p.supplement_num AS [Дополнение],
                p.index_num AS [Индекс],
                p.title AS [Название],
                p.date_start AS [Начало],
                p.ID_parent AS [id родителя]
            FROM tblPeriods AS p
            LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
            LEFT JOIN tblItems AS i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
            ORDER BY i.title, i.title, p.supplement_num DESC, p.index_num DESC;
    """,

    "select_period_by_title": """--sql
        SELECT * FROM tblPeriods WHERE title = ?;
    """,

}
