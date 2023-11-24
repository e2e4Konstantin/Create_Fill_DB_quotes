create table person (name text, parent text, status text);
insert into person (name, parent, status) values
    ('A', 'X', 'Alive'),
    ('B', 'Y', 'Dead'),
    ('X', 'X1', 'Alive'),
    ('Y', 'Y1', 'Alive'),
    ('X1', 'X2', 'Alive'),
    ('Y1', 'Y2', 'Dead');
select * from person;
select
    c.name as child,
    c.parent as parent,
    p.parent as grand_parent,
    g.status as grand_parent_status
from person as c
left join person as p on c.parent = p.name
left join person as g on p.parent = g.name;
