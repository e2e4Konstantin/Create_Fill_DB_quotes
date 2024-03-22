create table test (
    id integer,
    parent_id integer,
    name text
);

insert into test values
    (1, null, 'Адам'),
    (2, 1, 'Каин'),
    (3, 1, 'Авель'),
    (4, 2, 'Енох'),
    (5, 4, 'Ирад'),
    (6, 5, 'Мехиаель'),
    (7, 6, 'Мафусал'),
    (8, 7, 'Ламех');


with recursive m(depth, path, id, name) as (
    select 1, id path, id, name from test where parent_id is null
    union all
    select depth + 1, path || ',' || t.id, t.id, t.name 
    from test t, m where t.parent_id = m.id
) select * from m
where depth = 2 and id = 3;    


