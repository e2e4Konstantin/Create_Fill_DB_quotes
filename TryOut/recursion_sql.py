# https://habr.com/ru/articles/269497/
# https://wiki.postgresql.org/wiki/Main_Page/ru
# https://habr.com/ru/articles/27439/

create = """
    CREATE TABLE geo (
        id integer not null primary key,
        parent_id integer references geo(id),
        name text
    );
"""
insert  = """
    INSERT INTO geo
    (id, parent_id, name)
    VALUES
    (1, null, 'Планета Земля'),
    (2, 1, 'Континент Евразия'),
    (3, 1, 'Континент Северная Америка'),
    (4, 2, 'Европа'),
    (5, 4, 'Россия'),
    (6, 4, 'Германия'),
    (7, 5, 'Москва'),
    (8, 5, 'Санкт-Петербург'),
    (9, 6, 'Берлин');
"""

recurs = """
    WITH RECURSIVE r AS ( SELECT id, parent_id, name FROM geo WHERE parent_id = 4
       UNION
       SELECT geo.id, geo.parent_id, geo.name
       FROM geo
       JOIN r ON geo.parent_id = r.id
    )
    SELECT * FROM r;
"""
