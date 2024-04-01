sql_catalog_queries = {

    # -- >  DELETE ----------------------------------------------------------------------
    "delete_catalog_last_periods": """
        DELETE FROM tblCatalogs
        WHERE ID_tblCatalog IN (
                SELECT ID_tblCatalog
                FROM tblCatalogs
                WHERE FK_tblCatalogs_tblPeriods < ?);
    """,

    "delete_catalog_less_than_specified_supplement_period": """--sql
        /*
        удаляет записи каталога у которых родитель code=?, тип origin=?
        и номер дополнения периода < ?
        */
        DELETE FROM tblCatalogs WHERE tblCatalogs.ID_tblCatalog IN (
            --
            SELECT cat.ID_tblCatalog
            FROM tblCatalogs AS cat
            JOIN tblPeriods AS per ON per.ID_tblPeriod = cat.FK_tblCatalogs_tblPeriods
            WHERE cat.ID_tblCatalog IN (
                --
                WITH CatalogLevel AS (
                    SELECT ID_tblCatalog, ID_parent
                    FROM tblCatalogs
                    WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?
                    UNION ALL
                        SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                        JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
                )
                SELECT ID_tblCatalog from CatalogLevel
                --
                ) AND
                per.supplement_num < ?
            --
        );
    """,

    # -- >  SELECT ----------------------------------------------------------------------
    "select_catalog_id_code": """--sql
        SELECT ID_tblCatalog FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?;
    """,

    "select_catalog_id": """
        SELECT * FROM tblCatalogs WHERE ID_tblCatalog = ?;
    """,

    "select_catalog_code": """
        SELECT * FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?;
        """,
    "select_catalog_max_period": """
        SELECT MAX(FK_tblCatalogs_tblPeriods) AS max_period FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ?;
    """,

    "select_count_last_period": """
        SELECT COUNT(*) FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND FK_tblCatalogs_tblPeriods < ?;
    """,

    "select_changes": """--sql
        SELECT CHANGES() AS changes;
    """,

    # выводит все дочерние записи каталога для записи с нужным code
    # все записи для которых родителем является запись с code
    "select_catalog_level": """--sql
        WITH CatalogLevel AS (
            SELECT ID_tblCatalog, ID_parent
                FROM tblCatalogs
                WHERE code = ?
            UNION ALL
            SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
        )
        SELECT ID_tblCatalog from CatalogLevel;
     """,

    # "select_catalog_max_level_period": """--sql
    #     SELECT MAX(m.FK_tblCatalogs_tblPeriods) AS max_period
    #     FROM tblCatalogs m
    #     WHERE m.ID_tblCatalog IN (
    #         WITH CatalogLevel AS (
    #             SELECT ID_tblCatalog, ID_parent from tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?
    #             UNION ALL
    #             SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
    #             JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
    #         )
    #         SELECT ID_tblCatalog from CatalogLevel
    #     );
    # """,

    "select_catalog_max_supplement_period": """--sql
        /*
        считает максимальное значение дополнения для периода
        записей каталога у которых родитель code и тип origin
        */
        SELECT MAX(per.supplement_num) AS max_supplement_periods
        FROM tblCatalogs AS cat
        JOIN tblPeriods AS per ON per.ID_tblPeriod = cat.FK_tblCatalogs_tblPeriods
        WHERE cat.ID_tblCatalog IN (
            --
            WITH CatalogLevel AS (
                SELECT ID_tblCatalog, ID_parent
                FROM tblCatalogs
                WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?
                UNION ALL
                    SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                    JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
            )
            SELECT ID_tblCatalog from CatalogLevel
            --
        );
    """,


    "select_catalog_count_period_supplement": """--sql
        /*
        считает количество записей каталога у которых родитель code=? тип origin=?
        и номер дополнения периода < ?
        */
        SELECT COUNT(cat.ID_tblCatalog) AS count
        FROM tblCatalogs AS cat
        JOIN tblPeriods AS per ON per.ID_tblPeriod = cat.FK_tblCatalogs_tblPeriods
        WHERE cat.ID_tblCatalog IN (
            --
            WITH CatalogLevel AS (
                SELECT ID_tblCatalog, ID_parent
                FROM tblCatalogs
                WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?
                UNION ALL
                    SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                    JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
            )
            SELECT ID_tblCatalog from CatalogLevel
            --
       ) AND
        per.supplement_num < ?;
    """,


    # -- >  INSERT ----------------------------------------------------------------------
    "insert_catalog": """--sql
        INSERT INTO tblCatalogs (
            FK_tblCatalogs_tblOrigins, ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """,

    # -- >  UPDATE ----------------------------------------------------------------------
    "update_catalog_id": """--sql
        UPDATE tblCatalogs
        SET (
            FK_tblCatalogs_tblOrigins, ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code
        ) = (?, ?, ?, ?, ?, ?, ?)
        WHERE ID_tblCatalog = ?;
    """,

    "update_catalog_period_main_row": """
        UPDATE tblCatalogs SET (FK_tblCatalogs_tblPeriods) = (?) WHERE code = ? AND FK_tblCatalogs_tblOrigins = ?;
    """,

    "update_catalog_parent_himself": """--sql
        UPDATE tblCatalogs SET (ID_parent) = (ROWID) WHERE ID_tblCatalog = ?;
    """,


}

sql_catalog_creates = {

    # --- > Каталог ---------------------------------------------------------------------------

    "delete_table_catalog": """DROP TABLE IF EXISTS tblCatalogs;""",
    "delete_index_catalog": """DROP INDEX IF EXISTS idxCatalogsCode;""",
    "delete_view_catalog": """DROP VIEW IF EXISTS viewCatalog;""",

    "delete_table_catalog_history": """DROP TABLE IF EXISTS _tblHistoryCatalogs;""",
    "delete_index_catalog_history": """DROP INDEX IF EXISTS idxHistoryCatalogs;""",

    "create_table_catalogs": """--sql
        CREATE TABLE IF NOT EXISTS tblCatalogs
            (
                ID_tblCatalog               INTEGER PRIMARY KEY NOT NULL,
                FK_tblCatalogs_tblOrigins   INTEGER NOT NULL, -- происхождение ТСН/ПСМ...
                ID_parent                   INTEGER REFERENCES tblCatalogs (ID_tblCatalog) NOT NULL,  -- ссылка родителя
                FK_tblCatalogs_tblPeriods   INTEGER NOT NULL, -- период на который загружен каталог
                code                        TEXT NOT NULL,    -- шифр элемента каталога
                description                 TEXT NOT NULL,    -- описание
                FK_tblCatalogs_tblItems     INTEGER NOT NULL, -- тип элемента каталога
                digit_code                  INTEGER NOT NULL, -- шифр преобразованный в число
                last_update                 INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')), -- время обновления
                FOREIGN KEY (FK_tblCatalogs_tblOrigins) REFERENCES tblOrigins (ID_tblOrigin),
                FOREIGN KEY (FK_tblCatalogs_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
                FOREIGN KEY (FK_tblCatalogs_tblItems) REFERENCES tblItems (ID_tblItem),
                UNIQUE (FK_tblCatalogs_tblOrigins, FK_tblCatalogs_tblPeriods, code)
            );
        """,

    "create_index_catalog": """--sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxCatalogs ON tblCatalogs (
            FK_tblCatalogs_tblOrigins, FK_tblCatalogs_tblPeriods, code, FK_tblCatalogs_tblItems, digit_code
        );
    """,

    "create_table_history_catalog": """--sql
        CREATE TABLE IF NOT EXISTS _tblHistoryCatalogs (
            _rowid                      INTEGER,
            ID_tblCatalog               INTEGER,
            FK_tblCatalogs_tblOrigins   INTEGER,
            ID_parent                   INTEGER,
            FK_tblCatalogs_tblPeriods   INTEGER,
            code                        TEXT,
            description                 TEXT,
            FK_tblCatalogs_tblItems     INTEGER,
            digit_code                  INTEGER,
            last_update                 INTEGER,
            _version                    INTEGER NOT NULL,
            _updated                    INTEGER NOT NULL,
            _mask                       INTEGER NOT NULL
        );
        """,

    "create_index_history_catalog": """--sql
        CREATE INDEX IF NOT EXISTS idxHistoryCatalogs ON _tblHistoryCatalogs (_rowid);
    """,

    "create_trigger_history_catalog_insert": """--sql
        CREATE TRIGGER IF NOT EXISTS tgrHistoryCatalogsInsert
        AFTER INSERT ON tblCatalogs
        BEGIN
            INSERT INTO _tblHistoryCatalogs (
                _rowid, ID_tblCatalog, FK_tblCatalogs_tblOrigins,
                ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblCatalog, new.FK_tblCatalogs_tblOrigins,
                new.ID_parent, new.FK_tblCatalogs_tblPeriods, new.code, new.description,
                new.FK_tblCatalogs_tblItems, new.digit_code, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_catalog_delete": """--sql
        CREATE TRIGGER tgrHistoryCatalogsDelete
        AFTER DELETE ON tblCatalogs
        BEGIN
            INSERT INTO _tblHistoryCatalogs (
                _rowid, ID_tblCatalog, FK_tblCatalogs_tblOrigins,
                ID_parent, FK_tblCatalogs_tblPeriods, code, description, FK_tblCatalogs_tblItems, digit_code, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblCatalog, old.FK_tblCatalogs_tblOrigins,
                old.ID_parent, old.FK_tblCatalogs_tblPeriods, old.code, old.description,
                old.FK_tblCatalogs_tblItems, old.digit_code, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryCatalogs WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_catalog_update": """--sql
        CREATE TRIGGER IF NOT EXISTS tgrHistoryCatalogsUpdate
        AFTER UPDATE ON tblCatalogs
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryCatalogs (
                _rowid, ID_tblCatalog, FK_tblCatalogs_tblOrigins,
                ID_parent, FK_tblCatalogs_tblPeriods, code, description,
                FK_tblCatalogs_tblItems, digit_code, last_update, _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblCatalog != new.ID_tblCatalog THEN new.ID_tblCatalog ELSE null END,
                CASE WHEN old.FK_tblCatalogs_tblOrigins != new.FK_tblCatalogs_tblOrigins THEN new.FK_tblCatalogs_tblOrigins ELSE null END,
                CASE WHEN old.ID_parent != new.ID_parent THEN new.ID_parent ELSE null END,
                CASE WHEN old.FK_tblCatalogs_tblPeriods != new.FK_tblCatalogs_tblPeriods THEN new.FK_tblCatalogs_tblPeriods ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.FK_tblCatalogs_tblItems != new.FK_tblCatalogs_tblItems THEN new.FK_tblCatalogs_tblItems ELSE null END,
                CASE WHEN old.digit_code != new.digit_code THEN new.digit_code ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryCatalogs WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                (CASE WHEN old.ID_tblCatalog != new.ID_tblCatalog then 1 else 0 END) +
                (CASE WHEN old.FK_tblCatalogs_tblOrigins != new.FK_tblCatalogs_tblOrigins then 2 else 0 END) +
                (CASE WHEN old.ID_parent != new.ID_parent then 4 else 0 END) +
                (CASE WHEN old.FK_tblCatalogs_tblPeriods != new.FK_tblCatalogs_tblPeriods then 8 else 0 END) +
                (CASE WHEN old.code != new.code then 16 else 0 END) +
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.FK_tblCatalogs_tblItems != new.FK_tblCatalogs_tblItems then 64 else 0 END) +
                (CASE WHEN old.digit_code != new.digit_code then 128 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 256 else 0 END)
            WHERE
                old.ID_tblCatalog != new.ID_tblCatalog OR
                old.FK_tblCatalogs_tblOrigins != new.FK_tblCatalogs_tblOrigins OR
                old.ID_parent != new.ID_parent OR
                old.FK_tblCatalogs_tblPeriods != new.FK_tblCatalogs_tblPeriods OR
                old.code != new.code OR
                old.description != new.description OR
                old.FK_tblCatalogs_tblItems != new.FK_tblCatalogs_tblItems OR
                old.digit_code != new.digit_code OR
                old.last_update != new.last_update;
        END;
    """,

    "create_view_catalog": """--sql
        CREATE VIEW viewCatalog AS
            SELECT
                o.name AS 'источник',
                per.title AS 'период',
                i.title AS 'тип',
                m.code AS 'шифр',
                m.description AS 'описание',

                (SELECT i.title
                FROM tblCatalogs p
                LEFT JOIN tblItems i ON i.ID_tblItem = p.FK_tblCatalogs_tblItems
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'тип родителя',

                (SELECT p.code
                FROM tblCatalogs p
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'шифр родителя',

               (SELECT p.description
                FROM tblCatalogs p
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'описание родителя',

                m.digit_code AS 'числ.шифр'

            FROM tblCatalogs m
            LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = m.FK_tblCatalogs_tblOrigins
            LEFT JOIN tblItems AS i ON i.ID_tblItem = m.FK_tblCatalogs_tblItems
            LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblCatalogs_tblPeriods
            ORDER BY m.digit_code;
    """,

}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
