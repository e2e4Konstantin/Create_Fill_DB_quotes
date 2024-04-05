
SELECT * FROM larix."period" WHERE title='Дополнение 69';
-- 167085727

SELECT
	tr.title "type",
	sc.type_resource "id_type_resource",
	sc."period" "id_period",
	per."title" "title_period",
	sc."id" "id",
	sc.title,
	sc.rate,
	sc.cmt
FROM larix.storage_cost sc
INNER JOIN larix.type_resource tr ON tr.id = sc.type_resource 
INNER JOIN larix."period" per on per."id" = sc.PERIOD AND per.deleted_on IS NULL
WHERE sc.deleted_on IS NULL AND sc."period" IN (150862302, 150996873, 151248691,	151569296, 151763529)
ORDER BY per.created_on DESC 
--LIMIT 10
;


SELECT
	per."title" "title_period", sc."id", 
	sc."period" "id_period",
	sc.*
FROM larix.storage_cost sc
INNER JOIN larix."period" per on per."id" = sc."period" AND per.deleted_on IS NULL
WHERE sc.deleted_on IS NULL AND sc."period" IN (
	150862302,
	150996873,
	151248691,
	151569296,
	151763529,
	151902634,
	151991579,
	152473013,
	152623191,
	153689235,
	166956935,
	166998701,
	167264731
)
ORDER BY per.created_on DESC 

;


