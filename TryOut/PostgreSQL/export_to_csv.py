from icecream import ic

from psycopg2 import sql

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.config_db_postgres import PostgresDB



if __name__ == "__main__":
    with PostgresDB() as db:
        ic(db.connection)
        r = db.select_rows_dict_cursor('SELECT 1+1 AS sum2;')
        ic(r)
        r2 = db.select_rows_dict_cursor(
            pg_sql_queries["get_period_id"], 
            {'period_title': 'Дополнение 67'}
        )
        ic(r2)
        ic(list(r2[0].keys())) 
        ic(type(r2[0]))  # 'period_id'
        ic(r2[0]['period_id'])

        # print(pg_sql_queries["get_group_work_process_for_period_id"])

        output_query = f"COPY ({sql}) TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ';', FORCE_QUOTE *)"

        
        # r3 = db.select_rows_dict_cursor(
        #     pg_sql_queries["get_group_work_process_for_period_id"],
        #     {'period_id': 150719989}
        # )
        # ic(len(r3))

        pid = 150719989
        # query = sql.SQL(pg_sql_queries['q2']).format(
        #     period_id=sql.Identifier(str(pid)))
        # print(query)    

        # x = f"""COPY (
        #         SELECT gwp.* 
        #         FROM larix.group_work_process AS gwp 
        #         WHERE gwp.deleted = 0 AND gwp.period = {pid} 
        #         ORDER BY gwp.pressmark_sort LIMIT 10
        #     ) TO STDOUT WITH CSV DELIMITER ';'"""
        # print(x)

        # s = "'SELECT col1 FROM myDB.myTable WHERE col1 > 2'"

    x = pg_sql_queries['q1'].format(pid)
    print(x)
    # SQL_for_file_output = f"COPY ({0}) TO STDOUT WITH CSV HEADER"



        # query = sql.SQL("SELECT {field} AS period_id from {table} p WHERE {title} = %(period_title)s").format(
        #     field=sql.Identifier('p.id'), table=sql.Identifier('larix.period'), title=sql.Identifier('p.title'))

        # print(query.as_string(db.connection))

        # string = sql.SQL("""
        #     copy {} (value)
        #     from stdin (
        #     format csv,
        #     null "NULL",
        #     delimiter ',',
        #     header);
        #     """).format(sql.Identifier(TABLE))

        # sql = """COPY (SELECT * FROM larix.period p LIMIT 10) TO STDOUT WITH CSV DELIMITER ';'"""
        # ic(sql)

        # query = sql.SQL("COPY (SELECT * FROM {table} p LIMIT 10) TO STDOUT WITH CSV DELIMITER ';' ").format(
        #     table=sql.Identifier('larix.period'))
        # ic(query)


        # with open("test.csv", "w", encoding='utf-8') as file:
        #      db.cursor.copy_expert(x, file)
