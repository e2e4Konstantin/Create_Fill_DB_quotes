
create_view_catalog = """
CREATE VIEW viewCatalog AS
SELECT 
    m.period AS 'период',
    i.name AS 'тип', 
    m.code AS 'шифр', 
    m.description AS 'описание',

    (SELECT i.name
    FROM tblCatalogs p
    LEFT JOIN tblDirectoryItems i ON i.ID_tblDirectoryItem = p.FK_tblCatalogs_tblDirectoryItems
    WHERE p.ID_tblCatalog = m.ID_parent) AS 'тип родителя',
    
    (SELECT p.code
    FROM tblCatalogs p
    WHERE p.ID_tblCatalog = m.ID_parent) AS 'шифр родителя',
    
   (SELECT p.description
    FROM tblCatalogs p
    WHERE p.ID_tblCatalog = m.ID_parent) AS 'описание родителя'
    
FROM tblCatalogs m 
LEFT JOIN tblDirectoryItems AS i ON i.ID_tblDirectoryItem = m.FK_tblCatalogs_tblDirectoryItems
ORDER BY m.code
"""