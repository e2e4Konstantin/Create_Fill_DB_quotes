pg_sql_queries = {

    "get_all_period_table": """--sql
        COPY (SELECT p.* FROM larix.period AS p) TO STDOUT WITH CSV HEADER DELIMITER ';';
    """,

    "get_all_periods": """--sql
        SELECT p.* FROM larix.period AS p;
    """,

    "get_period_id": """--sql 
        -- получить id периода с указанным Названием (title)
        --{'period_title': 'Дополнение 67'}
        SELECT p.id AS period_id from larix.period p WHERE p.title = %(period_title)s;
    """,

    "get_group_work_process_for_period_id": """--sql
        -- получить записи дерева расценок для id периода
        SELECT gwp.* 
        FROM larix.group_work_process AS gwp 
        WHERE gwp.deleted = 0 AND gwp.period = %(period_id)s 
        ORDER BY gwp.pressmark_sort;
    """,

    "get_work_process_for_period_id": """--sql
        SELECT 
            p.title AS period_name, wp.id, gwp.pressmark AS gwp_pressmark, wp.pressmark,
	        wp.title, uom.title AS unit_measure, wp.PERIOD AS period_id, wp.group_work_process
        FROM larix.work_process wp
        --
        INNER JOIN larix.period p on p.id=wp.period
        INNER JOIN larix.group_work_process gwp on gwp.id=wp.GROUP_WORK_PROCESS AND gwp.period=wp.period
        INNER JOIN larix.unit_of_measure uom on uom.id=wp.unit_of_measure
        --
        WHERE wp.deleted = 0 AND wp.period = %(period_id)s
        ORDER BY wp.pressmark_sort;
    """,


    # -------------- test -----------------------

    "q_test_1": """--sql 
        SELECT p.id AS period_id from larix.period p WHERE p.title = {0};
    """,

    "q_test_2": """--sql
        COPY (
            SELECT gwp.* 
            FROM larix.group_work_process AS gwp 
            WHERE gwp.deleted = 0 AND gwp.period = {period_id}%s 
            ORDER BY gwp.pressmark_sort LIMIT 10
        ) TO STDOUT WITH CSV DELIMITER ';'
    """,

    "": """--sql
        SELECT 1+1 AS sum;
    """,
}

