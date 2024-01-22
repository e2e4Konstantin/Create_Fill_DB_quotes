sql_catalog_queries = {

    # -- >  DELETE ----------------------------------------------------------------------
    "delete_catalog_last_periods": """
        DELETE FROM tblCatalogs 
        WHERE ID_tblCatalog IN (
                SELECT ID_tblCatalog 
                FROM tblCatalogs 
                WHERE period < ?);
    """,

    "delete_catalog_level_last_periods": """
        DELETE 
        FROM tblCatalogs 
        WHERE 
            tblCatalogs.ID_tblCatalog IN (
                WITH CatalogLevel AS (
                    SELECT ID_tblCatalog, ID_parent from tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?
                    UNION ALL 
                        SELECT c.ID_tblCatalog, c.ID_parent from tblCatalogs AS c
                        JOIN CatalogLevel ON c.ID_parent = CatalogLevel.ID_tblCatalog
                    ) 
                    SELECT ID_tblCatalog from CatalogLevel
            ) AND
            tblCatalogs.period < ?;    
    """,

    # -- >  SELECT ----------------------------------------------------------------------
    "select_catalog_id_code": """
        SELECT ID_tblCatalog FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?;
    """,

    "select_catalog_id": """
        SELECT * FROM tblCatalogs WHERE ID_tblCatalog = ?;
    """,

    "select_catalog_code": """
        SELECT * FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND code = ?;
        """,
    "select_catalog_max_period": """
        SELECT MAX(period) AS max_period FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ?;         
    """,

    "select_count_last_period": """
        SELECT COUNT(*) FROM tblCatalogs WHERE FK_tblCatalogs_tblOrigins = ? AND period < ?;         
    """,

    "select_changes": """
        SELECT CHANGES() AS changes;         
    """,

    # выводит все дочерние записи каталога для записи с нужным code
    # все записи для которых родителем является запись с code
    "select_catalog_level": """
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

    "select_catalog_max_level_period": """
        SELECT MAX(m.period) AS max_period 
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
    """,

    "select_catalog_count_level_period": """
    SELECT COUNT(m.period) AS count 
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
    """,




    # -- >  INSERT ----------------------------------------------------------------------
    "insert_catalog": """
        INSERT INTO tblCatalogs (
            FK_tblCatalogs_tblOrigins, ID_parent, period, code, description, FK_tblCatalogs_tblItems
        ) 
        VALUES (?, ?, ?, ?, ?, ?);
    """,

    # -- >  UPDATE ----------------------------------------------------------------------
    "update_catalog_id": """
        UPDATE tblCatalogs 
        SET (
            FK_tblCatalogs_tblOrigins, ID_parent, period, code, description, FK_tblCatalogs_tblItems
        ) = (?, ?, ?, ?, ?, ?) 
        WHERE ID_tblCatalog = ?;
    """,

    "update_catalog_period_main_row": """
        UPDATE tblCatalogs SET (period) = (?) WHERE code = ? AND FK_tblCatalogs_tblOrigins = ?;
    """,

    "update_catalog_parent_himself": """
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

    "create_table_catalogs": """
        CREATE TABLE IF NOT EXISTS tblCatalogs
            (
                ID_tblCatalog               INTEGER PRIMARY KEY NOT NULL,
                FK_tblCatalogs_tblOrigins   INTEGER NOT NULL, -- происхождение ТСН/ПСМ...
                ID_parent     INTEGER REFERENCES tblCatalogs (ID_tblCatalog) NOT NULL,  -- ссылка родителя
                period        INTEGER NOT NULL,             -- период на который загружен каталог
                code	 	  TEXT NOT NULL,                -- шифр элемента каталога    								
                description	  TEXT NOT NULL,                -- описание
                FK_tblCatalogs_tblItems INTEGER NOT NULL,   -- тип элемента каталога
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                FOREIGN KEY (FK_tblCatalogs_tblItems) REFERENCES tblItems (ID_tblItem),
                UNIQUE (FK_tblCatalogs_tblOrigins, code)
            );
        """,

    "create_index_catalog": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxCatalogs ON tblCatalogs (
            FK_tblCatalogs_tblOrigins, code, period, FK_tblCatalogs_tblItems 
        );
    """,

    "create_table_history_catalog": """
        CREATE TABLE IF NOT EXISTS _tblHistoryCatalogs (
            _rowid        INTEGER,
            ID_tblCatalog INTEGER,
            FK_tblCatalogs_tblOrigins INTEGER,
            ID_parent     INTEGER,
            period        INTEGER,
            code          TEXT,
            description   TEXT,
            FK_tblCatalogs_tblItems INTEGER,
            last_update   INTEGER,          
            _version      INTEGER NOT NULL,
            _updated      INTEGER NOT NULL,
            _mask         INTEGER NOT NULL
        );
        """,

    "create_index_history_catalog": """
        CREATE INDEX IF NOT EXISTS idxHistoryCatalogs ON _tblHistoryCatalogs (_rowid);
    """,

    "create_trigger_history_catalog_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryCatalogsInsert
        AFTER INSERT ON tblCatalogs
        BEGIN
            INSERT INTO _tblHistoryCatalogs (
                _rowid, ID_tblCatalog, FK_tblCatalogs_tblOrigins, 
                ID_parent, period, code, description, FK_tblCatalogs_tblItems, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_tblCatalog, new.FK_tblCatalogs_tblOrigins, 
                new.ID_parent, new.period, new.code, new.description, 
                new.FK_tblCatalogs_tblItems, new.last_update, 
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_catalog_delete": """
        CREATE TRIGGER tgrHistoryCatalogsDelete
        AFTER DELETE ON tblCatalogs
        BEGIN
            INSERT INTO _tblHistoryCatalogs (
                _rowid, ID_tblCatalog, FK_tblCatalogs_tblOrigins, 
                ID_parent, period, code, description, FK_tblCatalogs_tblItems, last_update, 
                _version, _updated, _mask 
            )
            VALUES (
                old.rowid, old.ID_tblCatalog, old.FK_tblCatalogs_tblOrigins, 
                old.ID_parent, old.period, old.code, old.description,
                old.FK_tblCatalogs_tblItems, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryCatalogs WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_catalog_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryCatalogsUpdate
        AFTER UPDATE ON tblCatalogs
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryCatalogs (
                _rowid, ID_tblCatalog, FK_tblCatalogs_tblOrigins, 
                ID_parent, period, code, description, 
                FK_tblCatalogs_tblItems, last_update, _version, _updated, _mask 
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_tblCatalog != new.ID_tblCatalog THEN new.ID_tblCatalog ELSE null END,
                CASE WHEN old.FK_tblCatalogs_tblOrigins != new.FK_tblCatalogs_tblOrigins THEN new.FK_tblCatalogs_tblOrigins ELSE null END,
                CASE WHEN old.ID_parent != new.ID_parent THEN new.ID_parent ELSE null END,
                CASE WHEN old.period != new.period THEN new.period ELSE null END,
                CASE WHEN old.code != new.code THEN new.code ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.FK_tblCatalogs_tblItems != new.FK_tblCatalogs_tblItems THEN new.FK_tblCatalogs_tblItems ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryCatalogs WHERE _rowid = old.rowid) + 1, 
                unixepoch('now'),
                (CASE WHEN old.ID_tblCatalog != new.ID_tblCatalog then 1 else 0 END) +
                (CASE WHEN old.FK_tblCatalogs_tblOrigins != new.FK_tblCatalogs_tblOrigins then 2 else 0 END) +
                (CASE WHEN old.ID_parent != new.ID_parent then 4 else 0 END) +
                (CASE WHEN old.period != new.period then 8 else 0 END) +
                (CASE WHEN old.code != new.code then 16 else 0 END) +
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.FK_tblCatalogs_tblItems != new.FK_tblCatalogs_tblItems then 64 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 128 else 0 END)
            WHERE 
                old.ID_tblCatalog != new.ID_tblCatalog OR
                old.FK_tblCatalogs_tblOrigins != new.FK_tblCatalogs_tblOrigins OR
                old.ID_parent != new.ID_parent OR
                old.period != new.period OR
                old.code != new.code OR
                old.description != new.description OR
                old.FK_tblCatalogs_tblItems != new.FK_tblCatalogs_tblItems OR   
                old.last_update != new.last_update;
        END;
    """,

    "create_view_catalog": """
        CREATE VIEW viewCatalog AS
            SELECT
                o.name AS 'источник',
                m.period AS 'период',
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
                WHERE p.ID_tblCatalog = m.ID_parent) AS 'описание родителя'
                
            FROM tblCatalogs m 
            LEFT JOIN tblOrigins AS o ON o.ID_tblOrigin = m.FK_tblCatalogs_tblOrigins
            LEFT JOIN tblItems AS i ON i.ID_tblItem = m.FK_tblCatalogs_tblItems;
            --ORDER BY m.code
    """,

}

# SELECT code, description, substr(code, 1, 2), cast(code as cast), length(code), ABS(code)  FROM tblCatalogs;
# --order by substr(code, 1, 2) DESC;
#
#
# SELECT code, description, code REGEXP '^[5]' FROM tblCatalogs;
# --order by code GLOB '[0-9]';
