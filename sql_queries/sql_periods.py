#DROP TABLE IF EXISTS tblPeriods;


# ---> Периоды -----------------------------------------------------------------------
sql_periods_queries = {
    "get_index_periods_for_supplement_tsn": """--sql
        SELECT *
        FROM tblPeriods
        WHERE
            FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
            AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE team = 'periods_category' AND name = 'index')
            AND supplement_num = ?;

    """,
    # "get_periods_supplement_index_num": """--sql
    #     -- получить список периодов от n < дополнений < m и k < индексов < l
    #     SELECT p.ID_tblPeriod AS id, p.title AS title, p.basic_database_id AS basic_id
    #     FROM tblPeriods AS p
    #     WHERE p.supplement_num >= ? AND p.supplement_num <= ? AND p.index_num >= ? AND p.index_num <= ?
    #     ORDER BY p.supplement_num DESC, p.index_num DESC;
    # """,
    "select_period_id_by_normative_id": """--sql
        SELECT ID_tblPeriod FROM tblPeriods WHERE basic_database_id IS NOT NULL AND basic_database_id = ?;
    """,
    "select_period_by_normative_id": """--sql
        SELECT * FROM tblPeriods WHERE basic_database_id IS NOT NULL AND basic_database_id = ?;
    """,
    "get_periods_normative_id_index_num_more": """--sql
        -- получить список id индексных периодов для larix.Normative у которых index_num > нужного значения
        select basic_database_id, index_num, supplement_num
        from tblPeriods
        where
            FK_Origin_tblOrigins_tblPeriods is not null and
            FK_Category_tblItems_tblPeriods is not null and
            (select o.ID_tblOrigin from tblOrigins o where o.name = 'ТСН') is not null and
            (select i.ID_tblItem from tblItems i where i.team = 'periods_category' and i.name = 'index') is not null and
            FK_Origin_tblOrigins_tblPeriods = (select o.ID_tblOrigin from tblOrigins o where o.name = 'ТСН') and
            FK_Category_tblItems_tblPeriods = (select i.ID_tblItem from tblItems i where i.team = 'periods_category' and i.name = 'index') and
            index_num >= ?;

    """,
    "get_periods_supplement_num": """--sql
        -- получить список периодов для заданного каталога и тпа периода в диапазоне n < дополнений < m
        SELECT p.ID_tblPeriod AS [id], p.title AS [title], p.basic_database_id AS [basic_id],
            p.supplement_num AS [supplement], p.index_num AS [index]
        FROM tblPeriods AS p
        WHERE
        p.FK_Origin_tblOrigins_tblPeriods = ? AND
        p.FK_Category_tblItems_tblPeriods = ? AND
        p.supplement_num >= ? AND p.supplement_num <= ?
        ORDER BY p.supplement_num DESC;
    """,
    "insert_period": """--sql
        INSERT INTO tblPeriods (
            title, supplement_num, index_num, date_start, comment, ID_parent,
            FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    "insert_period_data": """--sql
        INSERT INTO tblPeriods (
            title, supplement_num, index_num, date_start, comment, ID_parent,
            FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, basic_database_id
        )
        VALUES ('Индекс 212', 72, 212, '2024-05-22', NULL, 70, 1, 29, 167597127);
    """,
    "update_periods_supplement_parent": """--sql
        -- Для дополнений ТСН устанавливает ID родительской записи где номер дополнения меньше на 1
        -- named style parameter: {"id_origin": 1, "id_item": 27}, 27 - supplement
        UPDATE tblPeriods
        SET ID_parent = p.ID_tblPeriod
        FROM (
            SELECT ID_tblPeriod, supplement_num
            FROM tblPeriods
            WHERE
                FK_Origin_tblOrigins_tblPeriods = :id_origin AND
                FK_Category_tblItems_tblPeriods = :id_item
        ) AS p
        WHERE
            p.supplement_num = tblPeriods.supplement_num - 1 AND
            tblPeriods.FK_Origin_tblOrigins_tblPeriods = :id_origin AND
            tblPeriods.FK_Category_tblItems_tblPeriods = :id_item;
        """,
    "update_periods_index_parent": """--sql
        -- Для индексов ТСН, устанавливает ID родительской записи на ту, где номер индекса меньше на 1.
        -- named style parameter: {"id_origin": 1, "id_item": 28}, 28 - index
        UPDATE tblPeriods
        SET ID_parent = p.ID_tblPeriod
        FROM (
            SELECT ID_tblPeriod, index_num
            FROM tblPeriods
            WHERE
                FK_Origin_tblOrigins_tblPeriods = :id_origin AND
                FK_Category_tblItems_tblPeriods = :id_item
        ) AS p
        WHERE
            p.index_num = tblPeriods.index_num - 1 AND
            FK_Origin_tblOrigins_tblPeriods = :id_origin AND
            FK_Category_tblItems_tblPeriods = :id_item;
        """,
    "update_periods_index_num_by_max": """--sql
        -- Для дополнений ТСН/Оборудование устанавливает номер индекса
        -- на максимальный из группы индексов предыдущего дополнения
        -- named style parameter:
        -- {"id_origin": 1, "id_item_index": 28, "id_item_supplement": 27}
        UPDATE tblPeriods
        SET index_num = p.max_num
        FROM (
            SELECT max(u.index_num) as max_num, u.supplement_num
            FROM tblPeriods u
            WHERE
                u.FK_Origin_tblOrigins_tblPeriods = :id_origin AND
                u.FK_Category_tblItems_tblPeriods = :id_item_index
            GROUP BY u.supplement_num
        ) AS p
        WHERE
            p.supplement_num = tblPeriods.supplement_num - 1 AND
            tblPeriods.FK_Origin_tblOrigins_tblPeriods = :id_origin AND
            tblPeriods.FK_Category_tblItems_tblPeriods = :id_item_supplement;
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
        basic_database_id               INTEGER,            -- id основной бд (Postgres Normative)
        last_update                     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
        FOREIGN KEY (FK_Origin_tblOrigins_tblPeriods) REFERENCES tblOrigins (ID_tblOrigin),
        FOREIGN KEY (FK_Category_tblItems_tblPeriods) REFERENCES tblItems (ID_tblItem),

        UNIQUE (FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, supplement_num, index_num, title)
    );
    """,
    "create_index_periods": """--sql
        CREATE INDEX idxPeriods ON tblPeriods (
            FK_Origin_tblOrigins_tblPeriods, FK_Category_tblItems_tblPeriods, supplement_num, index_num
        );
    """,
    "create_view_periods": """--sql
        CREATE VIEW viewPeriods AS
            SELECT
                p.ID_tblPeriod AS [id],
                o.name AS [Раздел],
                i.title AS [Категория],
                p.supplement_num AS [Дополнение],
                p.index_num AS [Индекс],
                p.title AS [Название],
                p.date_start AS [Начало],
                p.ID_parent AS [id родителя],
                s.title AS [родитель]
            FROM tblPeriods AS p
            LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
            LEFT JOIN tblItems AS i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
            LEFT JOIN tblPeriods AS s ON s.ID_tblPeriod = p.ID_parent
            ORDER BY
                p.FK_Origin_tblOrigins_tblPeriods ASC,
                p.supplement_num DESC,
                p.index_num DESC;
    """,
    "select_period_by_title": """--sql
        SELECT * FROM tblPeriods WHERE title = ?;
    """,
    "select_period_by_id": """--sql
        SELECT * FROM tblPeriods WHERE ID_tblPeriod = ?;
    """,
    "select_period_by_origin_and_numbers": """--sql
         SELECT * FROM tblPeriods WHERE FK_Origin_tblOrigins_tblPeriods = ? AND supplement_num = ? AND index_num = ?;
    """,
}
