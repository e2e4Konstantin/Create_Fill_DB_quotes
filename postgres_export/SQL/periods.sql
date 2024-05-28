
WITH vars AS (
    SELECT
        (SELECT p.id from larix.period p WHERE p.title = 'Дополнение 67') AS id67,
        (SELECT p.id from larix.period p WHERE p.title = 'Дополнение 68') AS id68
)
SELECT *
FROM larix.group_resource AS gp, vars
WHERE gp.PERIOD = vars.id67
LIMIT 5;


SELECT * FROM larix."period" WHERE title ~'^\s*Дополнение\s+(\d+|\d+\.\d+)\s*$'
;


SELECT per.* FROM larix."period" per WHERE per.title ~'^\s*\d+\s+индекс\/дополнение\s+\d+\s+\(.+\)\s*$'
ORDER BY per.created_on DESC 
;
--167264731

SELECT per.*  FROM larix."period" per WHERE per.deleted_on IS NULL AND per."id" = 167264731;

SELECT per.*  FROM larix."period" per WHERE per.deleted_on IS NULL AND per.title ~'^\s*Мониторинг'
ORDER BY per.created_on DESC 
;
--


SELECT * FROM larix."period" WHERE title ~'^\s*211 индекс/дополнение 72';
SELECT * FROM larix."period" WHERE title ~'Индекс май/дополнение 72';

