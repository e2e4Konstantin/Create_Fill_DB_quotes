-- https://www.sqlite.org/lang_with.html#rcex1

CREATE TABLE MyStruct (
  TMPLID text,
  REF_TMPLID text
);

INSERT INTO MyStruct
  (TMPLID, REF_TMPLID)
VALUES
  ('Root', NULL),
  ('Item1', 'Root'),
  ('Item2', 'Root'),
  ('Item3', 'Item1'),
  ('Item4', 'Item1'),
  ('Item5', 'Item2'),
  ('Item6', 'Item5');

select * from MyStruct;

WITH RECURSIVE
  under_root(name,level) AS (
    VALUES('Root',0)
    UNION ALL
    SELECT tmpl.TMPLID, under_root.level+1
      FROM MyStruct as tmpl JOIN under_root ON tmpl.REF_TMPLID=under_root.name
     ORDER BY 2 DESC
  )
SELECT substr('....................',1,level*3) || name as TreeStructure FROM under_root;

WITH RECURSIVE
  cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<100)
SELECT x FROM cnt;
