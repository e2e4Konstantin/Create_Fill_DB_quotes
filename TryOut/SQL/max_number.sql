CREATE TABLE IF NOT EXISTS t2 (code TEXT, title TEXT, numb INTEGER);
 
select *, hex(t1.bin) as n from t1 order by bin;
select *, hex(t1.bin) as n from t1 order by n;



select hex('0x29a2241af62bffff');
select hex('29a2241af62bffff') as [3], hex('29a2241af62c0000') as [3.0];


INSERT INTO t2 (code, title) VALUES 
('3', '29a2241af62bffff'),
('3.0', '29a2241af62c0000'),
('3.0-1', '29a22503cad11000');

UPDATE t2 SET numb = CAST(hex(t2.title) as integer);
SELECT 999999999999999999999+1;
select 9223372036854775807;


