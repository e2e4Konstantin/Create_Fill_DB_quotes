CREATE TABLE tblStorageCosts
            -- таблица для % Заготовительно-складских расходов (%ЗСР)
            (
                ID_tblStorageCosts              INTEGER PRIMARY KEY NOT NULL,
                FK_tblStorageCosts_tblItems     INTEGER NOT NULL, -- тип материал/машина/расценка/оборудование
                FK_tblStorageCosts_tblPeriods   INTEGER NOT NULL, -- id периода
                name                            TEXT NOT NULL,  -- Наименование
                percent_storage_costs           REAL NOT NULL DEFAULT 0.0
                                                CHECK(percent_storage_costs >= 0 AND percent_storage_costs <= 100), -- Процент ЗСР
                description                     TEXT NOT NULL,  -- подробное описание
                last_update                     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                UNIQUE (FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name)
            );

CREATE UNIQUE INDEX IF NOT EXISTS idxStorageCosts ON tblStorageCosts (FK_tblStorageCosts_tblPeriods, name);

CREATE TABLE _tblHistoryStorageCosts (
        -- ID_tblStorageCosts, FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
        -- name, percent_storage_costs, description, last_update
        -- таблица для хранения истории %ЗСР
            _rowid                          INTEGER,
            ID_tblStorageCosts              INTEGER,
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

CREATE INDEX IF NOT EXISTS idxHistoryStorageCosts ON _tblHistoryStorageCosts (_rowid);

CREATE TRIGGER tgrHistoryStorageCostsInsert
        AFTER INSERT ON tblStorageCosts
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCosts,
                FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
                name, percent_storage_costs, description, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblStorageCosts,
                new.FK_tblStorageCosts_tblItems, new.FK_tblStorageCosts_tblPeriods,
                new.name, new.percent_storage_costs, new.description,
                new.last_update, 1, unixepoch('now'), 0
            );
        END;
        
CREATE TRIGGER tgrHistoryStorageCostsDelete
        AFTER DELETE ON tblStorageCosts
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCosts,
                FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
                name, percent_storage_costs, description, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid, old.ID_tblStorageCosts,
                old.FK_tblStorageCosts_tblItems, old.FK_tblStorageCosts_tblPeriods,
                old.name, old.percent_storage_costs, old.description, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryStorageCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'), -1
            );
        END;
        
CREATE TRIGGER tgrHistoryStorageCostsUpdate
        AFTER UPDATE ON tblStorageCosts
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryStorageCosts (
                _rowid, ID_tblStorageCosts,
                FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods,
                name, percent_storage_costs, description, last_update,
                _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblStorageCosts != new.ID_tblStorageCosts THEN new.ID_tblStorageCosts ELSE null END,
                CASE WHEN old.FK_tblStorageCosts_tblItems != new.FK_tblStorageCosts_tblItems THEN new.FK_tblStorageCosts_tblItems ELSE null END,
                CASE WHEN old.FK_tblStorageCosts_tblPeriods != new.FK_tblStorageCosts_tblPeriods THEN new.FK_tblStorageCosts_tblPeriods ELSE null END,
                CASE WHEN old.name != new.name THEN new.name ELSE null END,
                CASE WHEN old.percent_storage_costs != new.percent_storage_costs THEN new.percent_storage_costs ELSE null END,
                CASE WHEN old.description != new.description THEN new.description ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryStorageCosts WHERE _rowid = old.rowid) + 1,
                UNIXEPOCH('now'),
                (CASE WHEN old.ID_tblStorageCosts != new.ID_tblStorageCosts then 1 else 0 END) +
                (CASE WHEN old.FK_tblStorageCosts_tblItems != new.FK_tblStorageCosts_tblItems then 2 else 0 END) +
                (CASE WHEN old.FK_tblStorageCosts_tblPeriods != new.FK_tblStorageCosts_tblPeriods then 4 else 0 END) +
                (CASE WHEN old.name != new.name then 8 else 0 END) +
                (CASE WHEN old.percent_storage_costs != new.percent_storage_costs then 16 else 0 END) +
                (CASE WHEN old.description != new.description then 32 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 64 else 0 END)
            WHERE
                old.ID_tblStorageCosts != new.ID_tblStorageCosts OR
                old.FK_tblStorageCosts_tblItems != new.FK_tblStorageCosts_tblItems OR
                old.FK_tblStorageCosts_tblPeriods != new.FK_tblStorageCosts_tblPeriods OR
                old.name != new.name OR
                old.percent_storage_costs != new.percent_storage_costs OR
                old.description != new.description OR
                old.last_update != new.last_update;
        END;

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



-- FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, 
-- name, percent_storage_costs, description

INSERT INTO tblStorageCosts (FK_tblStorageCosts_tblItems, FK_tblStorageCosts_tblPeriods, name, percent_storage_costs, description) VALUES 
	( 3, 166, 'Значение по умолчанию', 0, 'Значение по умолчанию, используется если не установлено действительное значение'),
	( 3, 166, 'Строительные материалы', 2, 'к строительным материалам, кроме металлических конструкций'),
	( 3, 166, 'Металлические конструкции', 0.75, 'к металлическим конструкциям'),
	( 4, 166, 'Оборудование', 1.2, 'к оборудованию');












