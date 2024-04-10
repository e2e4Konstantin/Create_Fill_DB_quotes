#
# -- % Заготовительно-складских расходов (%ЗСР)
#
sql_storage_costs_queries = {
    "insert_storage_cost": """--sql
        INSERT INTO tblStorageCosts
            (FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description)
        VALUES
            (?, ?, ?, ?, ?);
    """,
    "update_storage_cost": """--sql
        UPDATE tblStorageCosts
        SET
            FK_tblStorageCosts_tblItems = ?,
            FK_tblStorageCosts_tblPeriods = ?,
            name = ?,
            percent_storage_costs = ?,
            description = ?
        WHERE
            ID_tblStorageCost = ?;
    """,
    "select_storage_costs_item_name": """--sql
        SELECT p.index_num AS index_num, sc.*
        FROM tblStorageCosts sc
        JOIN tblPeriods p ON p.ID_tblPeriod = sc.FK_tblStorageCosts_tblPeriods
        WHERE sc.FK_tblStorageCosts_tblItems = ? AND sc.name = ?;

    """,
    "delete_table_storage_costs": """DROP TABLE IF EXISTS tblStorageCosts;""",
    "delete_index_storage_costs": """DROP INDEX IF EXISTS idxStorageCosts;""",
    "delete_table_history_storage_costs": """DROP TABLE IF EXISTS _tblHistoryStorageCosts;""",
    "delete_index_history_storage_costs": """DROP INDEX IF EXISTS idxHistoryStorageCosts;""",
    "delete_view_storage_costs": """DROP VIEW IF EXISTS viewStorageCosts;""",
    #
    "create_table_storage_costs": """--sql
        CREATE TABLE tblStorageCosts
            -- таблица для % Заготовительно-складских расходов (%ЗСР)
            (
                ID_tblStorageCost               INTEGER PRIMARY KEY NOT NULL,
                FK_tblStorageCosts_tblItems     INTEGER NOT NULL, -- тип материал/машина/расценка/оборудование
                FK_tblStorageCosts_tblPeriods   INTEGER NOT NULL, -- id периода
                name                            TEXT NOT NULL,    -- наименование
                percent_storage_costs           REAL NOT NULL DEFAULT 0.0
                                                CHECK(percent_storage_costs >= 0 AND percent_storage_costs <= 100), -- Процент ЗСР
                description                     TEXT NOT NULL,  -- подробное описание
                last_update                     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                --
                FOREIGN KEY (FK_tblStorageCosts_tblItems) REFERENCES tblItems (ID_tblItem),
                FOREIGN KEY (FK_tblStorageCosts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
                --
                UNIQUE (FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name)
            );
        """,
    "create_index_purchasing_storage_costs": """--sql
        CREATE UNIQUE INDEX idxStorageCosts ON tblStorageCosts (FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name);
    """,
    # -----------------------------------------------------------------------------------------------------------------
    "create_table_history_storage_costs": """--sql
        CREATE TABLE _tblHistoryStorageCosts (
        -- ID_tblStorageCost, FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
        -- name, percent_storage_costs, description, last_update
        -- таблица для хранения истории %ЗСР
            _rowid                          INTEGER,
            ID_tblStorageCost               INTEGER,
            FK_tblStorageCosts_tblItems     INTEGER,
            FK_tblStorageCosts_tblPeriods   INTEGER,
            name                            TEXT,
            percent_storage_costs           REAL,
            description                     TEXT,
            last_update                     INTEGER,
            _version                        INTEGER NOT NULL,
            _updated                        INTEGER NOT NULL,
            _mask                           INTEGER NOT NULL
        );
        """,
    "create_index_history_storage_costs": """--sql
        CREATE INDEX idxHistoryStorageCosts ON _tblHistoryStorageCosts (_rowid);
    """,
    # -- ID_tblStorageCost, FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description, last_update
    "create_trigger_insert_storage_costs": """--sql
        CREATE TRIGGER tgrHistoryStorageCostsInsert
        AFTER INSERT ON tblStorageCosts
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCost,
                FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
                name, percent_storage_costs, description, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblStorageCost,
                new.FK_tblStorageCosts_tblItems, new.FK_tblStorageCosts_tblPeriods,
                new.name, new.percent_storage_costs, new.description,
                new.last_update, 1, unixepoch('now'), 0
            );
        END;
    """,
    "create_trigger_delete_storage_costs": """--sql
        CREATE TRIGGER tgrHistoryStorageCostsDelete
        AFTER DELETE ON tblStorageCosts
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCost,
                FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
                name, percent_storage_costs, description, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblStorageCost,
                old.FK_tblStorageCosts_tblItems, old.FK_tblStorageCosts_tblPeriods,
                old.name, old.percent_storage_costs, old.description, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryStorageCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'), -1
            );
        END;
    """,
    "create_trigger_update_storage_cost": """--sql
        -- обновление цены %ЗСР в таблице tblPropertiesMachines
        CREATE TRIGGER tgrHistoryStorageCostsUpdate
        AFTER UPDATE ON tblStorageCosts
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCost,
                FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
                name, percent_storage_costs, description, last_update,
                _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblStorageCost != new.ID_tblStorageCost THEN new.ID_tblStorageCost ELSE null END,
                CASE WHEN old.FK_tblStorageCosts_tblItems != new.FK_tblStorageCosts_tblItems THEN new.FK_tblStorageCosts_tblItems ELSE null END,
                CASE WHEN old.FK_tblStorageCosts_tblPeriods != new.FK_tblStorageCosts_tblPeriods THEN new.FK_tblStorageCosts_tblPeriods ELSE null END,
                CASE WHEN old.name != new.name THEN new.name ELSE null END,
                CASE WHEN old.percent_storage_costs != new.percent_storage_costs THEN new.percent_storage_costs ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryStorageCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'),
                (CASE WHEN old.ID_tblStorageCost != new.ID_tblStorageCost then 1 else 0 END) +
                (CASE WHEN old.FK_tblStorageCosts_tblItems != new.FK_tblStorageCosts_tblItems then 2 else 0 END) +
                (CASE WHEN old.FK_tblStorageCosts_tblPeriods != new.FK_tblStorageCosts_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.name != new.name then 8 else 0 END) +
                (CASE WHEN old.percent_storage_costs != new.percent_storage_costs then 16 else 0 END) +
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 64 else 0 END)
            WHERE
                old.ID_tblStorageCost != new.ID_tblStorageCost OR
                old.FK_tblStorageCosts_tblItems != new.FK_tblStorageCosts_tblItems OR
                old.FK_tblStorageCosts_tblPeriods != new.FK_tblStorageCosts_tblPeriods OR
                old.name != new.name OR
                old.percent_storage_costs != new.percent_storage_costs OR
                old.description != new.description OR
                old.last_update != new.last_update;
        END;
    """,
    "create_view_storage_costs": """--sql
        CREATE VIEW viewStorageCosts AS
            SELECT
                i.name AS 'тип',
                p.title AS 'период',
                sc.percent_storage_costs AS '%ЗСР',
                sc.name AS 'название',
                sc.description AS 'описание'
            FROM tblStorageCosts sc
            LEFT JOIN tblItems AS i ON i.ID_tblItem = sc.FK_tblStorageCosts_tblItems
            LEFT JOIN tblPeriods AS p ON p.ID_tblPeriod = sc.FK_tblStorageCosts_tblPeriods
            ORDER BY FK_tblStorageCosts_tblPeriods, sc.name;
    """,
}

