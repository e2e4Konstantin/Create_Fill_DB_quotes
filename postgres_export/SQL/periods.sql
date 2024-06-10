
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

SELECT per.*  FROM larix."period" per 
WHERE 
	per.deleted_on IS NULL
	AND per.period_type = 1
	AND per.is_infl_rate = 1
	AND per."id" IN (150862302, 150996873);

SELECT per.*  FROM larix."period" per WHERE per.deleted_on IS NULL AND per.title ~'^\s*Мониторинг'
ORDER BY per.created_on DESC 
;
--


SELECT * FROM larix."period" WHERE title ~'^\s*211 индекс/дополнение 72';
SELECT * FROM larix."period" WHERE title ~'Индекс май/дополнение 72';
SELECT * FROM larix."period" WHERE title ~'^\s*Дополнение\s*69\s*$';
SELECT * FROM larix."period" WHERE title ~'^\s*210\s*индекс/дополнение\s*\d+\s*\(мониторинг';

SELECT * 
FROM larix."period" p
WHERE 
	p.deleted_on IS NULL
	AND date_start >= '2020-01-01'::date
	AND p.period_type = 1
--	AND p.is_infl_rate = 1
	AND p.title ~ '^\s*\d+\s*индекс/дополнение\s*\d+'
ORDER BY p.created_on DESC 	


SELECT 
	id,
	date_start,
	period_type,
	title,
	is_infl_rate,
	cmt,
	parent_id,
	previous_id,
	base_type_code
FROM larix.period
WHERE deleted_on IS NULL AND period_type = 1 AND date_start >= '2020-01-01'::date
	AND title ~ '^\s*[^ЕTФКТВНХ].+'
	
ORDER BY created_on DESC 	
;

SELECT 
	p.id AS "period_id",
	TRIM(SUBSTRING(p.title, 1, 4))::int AS "index_number",
	p.date_start "start_date",
--	,
--	p.previous_id 
	p.title "period_name"
FROM larix."period" p
WHERE 
	p.deleted_on IS NULL
	AND p.is_infl_rate = 1
	AND date_start >= '2020-01-01'::date
	--
	AND LOWER(p.title) ~ '^\s*\d+\s*индекс/дополнение\s*\d+\s*\(.+\)\s*$'
ORDER BY index_number	--p.date_start ASC
;



-- "^\s*Дополнение\s*73\s*$" 
SELECT 
	p.id AS "period_id",
	p.title "period_name"
FROM larix."period" p
WHERE 
	p.deleted_on IS NULL
	AND LOWER(p.title) ~ '^\s*дополнение\s*72\s*$'
;

 SELECT id, title
        FROM larix.period
        WHERE
            deleted_on IS NULL
            AND LOWER(title) ~ '^\s*дополнение\s*72\s*$'
        LIMIT 1;
       

