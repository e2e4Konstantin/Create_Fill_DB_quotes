# --- > Атрибуты ----------------------------------------------------------
sql_attributes_queries = {
    # --- > Период, к которому относится атрибут, будет взят из расценки/ресурса.
    "create_table_attributes": """
        CREATE TABLE IF NOT EXISTS tblAttributes
            (
                ID_Attribute                    INTEGER PRIMARY KEY NOT NULL,
                FK_tblAttributes_tblProducts    INTEGER NOT NULL,           -- id владельца 
                name                    	    TEXT NOT NULL,              -- название атрибута
                value                           TEXT NOT NULL,              -- значение атрибута
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	-- время последнего обновления
                FOREIGN KEY (FK_tblAttributes_tblProducts) REFERENCES tblProducts (ID_tblProduct),
                UNIQUE (FK_tblAttributes_tblProducts, name)
            );
    """,

    "create_index_attributes": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxAttributes ON tblAttributes (FK_tblAttributes_tblProducts, name);
    """,

    "create_view_attributes": """
        CREATE VIEW viewAttributes AS
            SELECT
                r.code AS 'шифр', 
                r.period AS 'период', 
                a.name AS 'атрибут',
                a.value AS 'значение'
            FROM tblAttributes a 
            LEFT JOIN tblProducts AS r ON r.ID_tblProduct = a.FK_tblAttributes_tblProducts
            ORDER BY r.code;
    """,

    "delete_table_attributes": """DROP TABLE IF EXISTS tblAttributes;""",
    "delete_index_attributes": """DROP INDEX IF EXISTS idxAttributes;""",
    "delete_view_attributes": """DROP VIEW IF EXISTS viewAttributes;""",
    "delete_table_history_attributes": """DROP TABLE IF EXISTS _tblHistoryAttributes;""",

    "insert_attribute": """
        INSERT INTO tblAttributes (FK_tblAttributes_tblProducts, name, value) VALUES (?, ?, ?);
    """,

    "delete_attributes_product_id_name": """
        DELETE FROM tblAttributes WHERE FK_tblAttributes_tblProducts =? AND name = ?;
    """,

    "delete_attributes_id": """
        DELETE FROM tblAttributes WHERE ID_Attribute = ?;
    """,

    "select_attributes_product_id_name": """
        SELECT * FROM tblAttributes WHERE FK_tblAttributes_tblProducts = ? AND name = ?;
    """,

    # --- > История изменения таблицы Атрибутов -----------------------------------
    "create_table_history_attributes": """
        CREATE TABLE IF NOT EXISTS _tblHistoryAttributes (
            _rowid          INTEGER,
            ID_Attribute    INTEGER,
            FK_tblAttributes_tblProducts INTEGER, 
            name            TEXT,
            value           TEXT, 
            last_update     INTEGER,          
            _version        INTEGER NOT NULL,
            _updated        INTEGER NOT NULL,
            _mask           INTEGER NOT NULL
        );
        """,

    "create_index_history_attributes": """
        CREATE INDEX IF NOT EXISTS idxHistoryAttributes ON _tblHistoryAttributes (_rowid);
    """,

    "create_trigger_history_attributes_insert": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryAttributesInsert
        AFTER INSERT ON tblAttributes
        BEGIN
            INSERT INTO _tblHistoryAttributes (
                _rowid, ID_Attribute, FK_tblAttributes_tblProducts, name, value, last_update,
                _version, _updated, _mask 
            )
            VALUES (
                new.rowid, new.ID_Attribute, new.FK_tblAttributes_tblProducts, 
                new.name, new.value, new.last_update, 
                1, unixepoch('now'), 0
            );
        END;
    """,

    "create_trigger_history_attributes_delete": """
        CREATE TRIGGER tgrHistoryAttributesDelete
        AFTER DELETE ON tblAttributes
        BEGIN
            INSERT INTO _tblHistoryAttributes (
                _rowid, ID_Attribute, FK_tblAttributes_tblProducts, name, value, last_update,
                _version, _updated, _mask 
            )
            VALUES (
                old.rowid, old.ID_Attribute, 
                old.FK_tblAttributes_tblProducts, old.name, old.value, old.last_update, 
                (SELECT COALESCE(MAX(_version), 0) FROM _tblHistoryAttributes WHERE _rowid = old.rowid) + 1,
                unixepoch('now'), -1
            );
        END;
    """,

    "create_trigger_history_attributes_update": """
        CREATE TRIGGER IF NOT EXISTS tgrHistoryAttributesUpdate
        AFTER UPDATE ON tblAttributes
        FOR EACH ROW
        BEGIN
            INSERT INTO _tblHistoryAttributes (
                _rowid, ID_Attribute, FK_tblAttributes_tblProducts, name, value, last_update,
                _version, _updated, _mask 
            )
            SELECT 
                old.rowid,
                CASE WHEN old.ID_Attribute != new.ID_Attribute THEN new.ID_Attribute ELSE null END,
                CASE WHEN old.FK_tblAttributes_tblProducts != new.FK_tblAttributes_tblProducts THEN new.FK_tblAttributes_tblProducts ELSE null END,
                CASE WHEN old.name != new.name THEN new.name ELSE null END,
                CASE WHEN old.value != new.value THEN new.value ELSE null END,
                CASE WHEN old.last_update != new.last_update THEN new.last_update ELSE null END,

                (SELECT MAX(_version) FROM _tblHistoryAttributes WHERE _rowid = old.rowid) + 1,
                unixepoch('now'),

                (CASE WHEN old.ID_Attribute != new.ID_Attribute then 1 else 0 END) +
                (CASE WHEN old.FK_tblAttributes_tblProducts != new.FK_tblAttributes_tblProducts then 2 else 0 END) +
                (CASE WHEN old.name != new.name then 4 else 0 END) +
                (CASE WHEN old.value != new.value then 8 else 0 END) +
                (CASE WHEN old.last_update != new.last_update then 16 else 0 END)
            WHERE 
                old.ID_Attribute != new.ID_Attribute OR
                old.FK_tblAttributes_tblProducts != new.FK_tblAttributes_tblProducts OR
                old.name != new.name OR
                old.value != new.value OR
                old.last_update != new.last_update;
        END;
    """,




}
