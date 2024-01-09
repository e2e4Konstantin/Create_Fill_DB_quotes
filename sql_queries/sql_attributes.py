sql_attributes_queries = {
    # --- > Атрибуты ----------------------------------------------------------
    # --- > Период, к которому относится атрибут, будет взят из расценки.
    "create_table_attributes": """
        CREATE TABLE IF NOT EXISTS tblAttributes
            (
                ID_Attribute                INTEGER PRIMARY KEY NOT NULL,
                FK_tblAttributes_tblProducts  INTEGER NOT NULL,               -- id родительской расценки 

                name                    	TEXT NOT NULL,                  -- название атрибута
                value                       TEXT NOT NULL,                  -- значение атрибута

                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	-- время последнего обновления
                FOREIGN KEY (FK_tblAttributes_tblProducts) REFERENCES tblProducts (ID_tblProduct),
                UNIQUE (FK_tblAttributes_tblProducts, name)
            );
    """,

    "create_index_attributes": """
        CREATE UNIQUE INDEX IF NOT EXISTS idxAttributes ON tblAttributes ( FK_tblAttributes_tblProducts, name);
    """,

    "create_view_attributes": """
        CREATE VIEW viewAttributes AS
            SELECT
                q.code AS 'шифр', 
                a.name AS 'атрибут',
                a.value AS 'значение'
            FROM tblAttributes a 
            LEFT JOIN tblProducts AS q ON q.ID_tblProduct = a.FK_tblAttributes_tblProducts
            ORDER BY q.code;
    """,


    "delete_table_attributes": """DROP TABLE IF EXISTS tblAttributes;""",
    "delete_index_attributes": """DROP INDEX IF EXISTS idxAttributes;""",
    "delete_view_attributes": """DROP VIEW IF EXISTS viewAttributes;""",

    "insert_attribute": """
        INSERT INTO tblAttributes (FK_tblAttributes_tblProducts, name, value) VALUES ( ?, ?, ?);
    """,

    "delete_attributes_quote_id_name": """
        DELETE FROM tblAttributes 
        WHERE FK_tblAttributes_tblProducts =? AND name = ?;
    """,

    "delete_attributes_id": """
        DELETE FROM tblAttributes WHERE ID_Attribute = ?;
    """,

    "select_attributes_quote_id_name":   """
        SELECT * FROM tblAttributes WHERE FK_tblAttributes_tblProducts = ? AND name = ?;
    """,

}

