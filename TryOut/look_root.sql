drop table tree;
create table tree (id integer primary key, p_id integer defoult NULL);
select * from tree;
INSERT INTO tree (p_id) values
    (NULL),
    (1),
    (1),
    (2),
    (2);

select id, p_id,
    case
        when p_id is NULL then 'Root'
        when p_id is not NULL and id in (select distinct p_id from tree) then 'Inner'
        else 'Leaf'
    end as Type
from tree;