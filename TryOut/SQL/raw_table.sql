SELECT * FROM tblRawData;

ALTER TABLE tblRawData ADD COLUMN index_number INTEGER;

SELECT substr(title_period,1,INSTR(title_period, ' ')-1), INSTR(title_period, ' ') FROM tblRawData;

SELECT CAST(substr(title_period,1,INSTR(title_period, ' ')) AS INTEGER)  FROM tblRawData;


UPDATE tblRawData
SET index_number = CAST(substr(title_period,1,INSTR(title_period, ' ')) AS INTEGER);

SELECT * FROM tblRawData ORDER BY index_number ASC;



SELECT p.index_num, sc.*
FROM tblStorageCosts sc
LEFT JOIN tblPeriods AS p ON p.ID_tblPeriod = sc.FK_tblStorageCosts_tblPeriods
WHERE sc.FK_tblStorageCosts_tblItems = 3 AND sc.name = '**';

SELECT sc.* FROM tblStorageCosts sc;
