CREATE VIEW vwInformationSchemaTables AS SELECT * FROM tblTransportCosts;

SELECT name, sql FROM sqlite_master
WHERE type='table'
ORDER BY name;

PRAGMA table_info(tblTransportCosts);