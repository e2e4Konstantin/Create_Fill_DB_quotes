
sql_items_queries = {
    "delete_all_data_items": """DELETE FROM tblItems;""",

    "select_items":  """SELECT * FROM tblItems;""",
    "select_items_id_team_name": """
        SELECT ID_tblItem FROM tblItems WHERE team IS ? and name IS ?;
    """,

    "select_items_all_team_name": """
        SELECT * FROM tblItems WHERE team IS ? and name IS ?;
    """,

    "select_item_id_team_name": """--sql 
        SELECT i.ID_tblItem FROM tblItems AS i WHERE i.team = ? AND  i.title = ?
    """,

    "select_items_team": """
        SELECT ID_tblItem, name, title, ID_parent, re_pattern
        FROM tblItems
        WHERE team IS ?;
    """,

    "select_items_dual_teams": """
        SELECT * FROM tblItems WHERE team IN (?, ?);
    """,
}

sql_items_creates = {
    "delete_table_items": """--sql
        DROP TABLE IF EXISTS tblItems;""",
    "delete_index_items": """--sql
        DROP INDEX IF EXISTS idxItems;""",

    "delete_table_items_history": """--sql
        DROP TABLE IF EXISTS _tblHistoryItems;""",
    "delete_index_items_history": """--sql
        DROP INDEX IF EXISTS idxHistoryItems;""",

    # --- > Таблица для хранения справочников ---------------------
    # названия справочников
    # ['units', 'quotes', 'materials', 'machines', 'equipments']
    #  в справочнике 'unit' хранятся категории для базовой таблицы Расценок, Материалов, Машин и Оборудования
    #  поле team определяет название справочника
    #  можно задать иерархию объектов справочника заполнив колонку ID_parent
    "create_table_items": """--sql
        CREATE TABLE IF NOT EXISTS tblItems (
                ID_tblItem  INTEGER PRIMARY KEY NOT NULL,
                team        TEXT NOT NULL,                              -- название справочника
                name        TEXT NOT NULL,                              -- название
                title     	TEXT NOT NULL,                              -- описание
                ID_parent   INTEGER REFERENCES tblItems (ID_tblItem),   -- родитель
                re_pattern  TEXT,                                       -- re паттерн шифра категории  NOT NULL
                re_prefix   TEXT,                                       -- re паттерн названия/title
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
                UNIQUE (team, name)
        );
    """,

    "create_index_items": """--sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxItems ON tblItems (team, name);
    """,

    "insert_item": """--sql
        INSERT INTO tblItems (team, name, title, ID_parent, re_pattern, re_prefix) VALUES ( ?, ?, ?, ?, ?, ?);
    """,

    # --- > таблица для хранения истории изменений Справочников ---------------------------
    # _mask битовая маска указывает в каком поле были изменения. Вычисляется как сумма значений:
    # 1:  ID_tblDirectoryItem
    # 2:  team
    # 4:  name
    # 8:  title
    # 16: ID_parent
    # 32: re_pattern
    # 64: re_prefix
    # 128: last_update
    # _mask равная -1 показывает что запись была удалена.
    "create_table_history_items": """--sql
        CREATE TABLE IF NOT EXISTS _tblHistoryItems (
            _rowid      INTEGER,
            ID_tblItem  INTEGER,
            team        TEXT,
            name        TEXT,
            title      	TEXT,
            ID_parent   INTEGER,
            re_pattern  TEXT,
            re_prefix   TEXT,
            last_update INTEGER,
            _version    INTEGER NOT NULL,
            _updated    INTEGER NOT NULL,
            _mask       INTEGER NOT NULL
        );
        """,

    "create_index_history_items": """--sql
        CREATE INDEX IF NOT EXISTS idxHistoryItems ON _tblHistoryItems (_rowid);
    """,

    "create_trigger_history_items_insert": """--sql
        CREATE TRIGGER IF NOT EXISTS tgrHistoryItemsInsert
        AFTER INSERT ON tblItems
        BEGIN
            INSERT INTO _tblHistoryItems (
                _rowid, ID_tblItem, team, name, title, ID_parent, re_pattern, re_prefix, last_update,
                _version, _updated, _mask
            )
            VALUES (
                new.rowid, new.ID_tblItem, new.team, new.name, new.title, new.ID_parent,
                new.re_pattern, new.re_prefix, new.last_update,
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_items_delete": """--sql
        CREATE TRIGGER tgrHistoryItemsDelete
        AFTER DELETE ON tblItems
        BEGIN
            INSERT INTO _tblHistoryItems (
                _rowid, ID_tblItem, team, name, title, ID_parent, re_pattern, re_prefix, last_update,
                _version, _updated, _mask
            )
            VALUES (
                old.rowid,
                old.ID_tblItem, old.team, old.name, old.title, old.ID_parent, old.re_pattern, old.re_prefix, old.last_update,
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryItems WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_items_update": """--sql
        CREATE TRIGGER IF NOT EXISTS tgrHistoryItemsUpdate
        AFTER UPDATE ON tblItems
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryItems (
                _rowid, ID_tblItem, team, name, title, ID_parent, re_pattern, re_prefix, last_update,
                _version, _updated, _mask
            )
            SELECT
                old.rowid,
                CASE WHEN old.ID_tblItem != new.ID_tblItem THEN new.ID_tblItem ELSE null END,
                CASE WHEN old.team != new.team THEN new.team ELSE null END,
                CASE WHEN old.code != new.name THEN new.name ELSE null END,
                CASE WHEN old.name != new.title THEN new.title ELSE null END,
                CASE WHEN old.ID_parent != new.ID_parent THEN new.ID_parent ELSE null END,
                CASE WHEN old.re_pattern != new.re_pattern THEN new.re_pattern ELSE null END,
                CASE WHEN old.re_prefix != new.re_prefix THEN new.re_prefix ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,
                (SELECT MAX(_version) FROM _tblHistoryItems WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),
                (CASE WHEN old.ID_tblItem != new.ID_tblItem then 1 else 0 END) +
                (CASE WHEN old.team != new.team then 2 else 0 END) +
                (CASE WHEN old.name != new.name then 4 else 0 END) +
                (CASE WHEN old.title != new.title then 8 else 0 END) +
                (CASE WHEN old.ID_parent != new.ID_parent then 16 else 0 END) +
                (CASE WHEN old.re_pattern != new.re_pattern then 32 else 0 END) +
                (CASE WHEN old.re_prefix != new.re_prefix then 64 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 128 else 0 END)
            WHERE
                old.ID_tblItem != new.ID_tblItem OR
                old.team != new.team OR
                old.name != new.name OR
                old.title != new.title OR
                old.ID_parent != new.ID_parent OR
                old.re_pattern != new.re_pattern OR
                old.re_prefix != new.re_prefix OR
                old.last_update != new.last_update;
        END;
    """,

}
