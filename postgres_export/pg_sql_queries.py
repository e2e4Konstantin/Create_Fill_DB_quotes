pg_sql_queries = {

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


    # -------------- test -----------------------
    "query_to_": """--sql
        COPY (SELECT * FROM larix.period p LIMIT 10) TO STDOUT WITH CSV DELIMITER ';';
    """,

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

