pg_sql_queries = {
    "get_all_period_table": """--sql
        COPY (SELECT p.* FROM larix.period AS p) TO STDOUT WITH CSV HEADER DELIMITER ';';
    """,
    "get_all_periods": """--sql
        SELECT p.* FROM larix.period AS p;
    """,
    # получить id периода с указанным Названием (title) # {'period_title': 'Дополнение 67'}
    "get_period_id": """--sql
        SELECT p.id AS period_id from larix.period p WHERE p.title = %(period_title)s;
    """,
    # {origin_title: 'ТСН'} {'ТСН', 'ОКП', 'ОКПД2'} тип справочника
    "get_origin_id": """--sql
        SELECT rc.id FROM larix.resource_classifier rc WHERE rc.title = %(origin_title)s;
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
    "get_group_resource_for_period_id_origin_id": """--sql
        -- получить записи дерева ресурсов для id периода и id resource_classifier 'ТСН'= 0
        -- 2 параметра (period_id, origin_id)
        SELECT
            rc.title "origin", per.title "period_name",
            gr.id, gr."period" "period_id",
            gr.parent "parent_id",  gr.pressmark, gr.title
        FROM larix.group_resource gr
        INNER JOIN larix.period per on per.id = gr.PERIOD
        INNER JOIN larix.resource_classifier rc ON rc.id = gr.resource_classifier
        WHERE gr.deleted = 0 AND gr."period" = %(period_id)s AND gr.pressmark !~ '(^\s*[0]+\s*$)' AND rc.id = %(origin_id)s
        ORDER BY gr.pressmark_sort;
    """,
    # получает ресурсы глава 1 и 2 для id периода
    "get_resources_1_2_for_period_id": """--sql
        SELECT
            r.id,
            r.pressmark ,
            r.title,
            uom.title "unit_measure",
            tr.title AS "type_resources",
            per.id "id_period",
            r.group_resource "id_group_resource",
            gr.pressmark "group_pressmark"
            /*,
            per.title AS "period_title",
            tr.title AS "type_resources",
            gr.title AS "group_title"
            */
        FROM larix.resources r
        INNER JOIN larix."period" per on per.id = r."period"
        INNER JOIN larix.group_resource gr ON gr.id = r.group_resource AND gr."period" = per.id
        INNER JOIN larix.type_resource tr ON tr.id = r.type_resource
        INNER JOIN larix.unit_of_measure uom ON uom.id = r.unit_of_measure
        WHERE r."period" = %(period_id)s AND r.deleted = 0 AND r.pressmark ~ '^\s*[1|2]\.'
        ORDER BY r.pressmark_sort
    """,
    "get_storage_costs_for_period_id_range": """--sql
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
        INNER JOIN larix."period" per ON per."id" = sc."period" AND per.deleted_on IS NULL
        WHERE sc.deleted_on IS NULL AND sc."period" IN %(period_id_range)s
        ORDER BY per.created_on DESC;
    """,
    "get_transport_costs_for_period_id_range": """--sql
        SELECT
            tc."id" "id",
            tc."period" "id_period",
            per.title "title_period",
            tc."title" "title",
            tc.pressmark,
            tc.price,
            tc.cur_price,
            tc.ratio,
            tc.cmt
            FROM larix.transport_cost tc
        INNER JOIN larix."period" per ON per."id" = tc."period" AND per.deleted_on IS NULL
        WHERE tc.deleted = 0 AND tc."period" IN %(period_id_range)s AND tc.pressmark ~ '(^\s*[1|2]\.)'
        ORDER BY per.created_on DESC;
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

