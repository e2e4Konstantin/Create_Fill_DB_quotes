
from icecream import ic
import pandas as pd
from psycopg2 import sql

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.config_db_postgres import PostgresDB
from postgres_export.pg_config import db_access


def query_to_csv(db: PostgresDB, csv_file_name: str, query: str, params=None) -> int:
    """ Получает выборку из БД и сохраняет ее в csv файл. """
    # df = pd.read_sql_query(
    #     sql=query, params=params, con=db.connection)
    results = db.select_rows_dict_cursor(query, params)
    if results:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        df.to_csv(csv_file_name, mode='x', encoding='utf-8', header=True, index=False)
        return 0
    return 1    
        

    
    




if __name__ == "__main__":
    with PostgresDB(db_access['vlad']) as db:
        ic(db.connection.dsn)

        r = db.select_rows_dict_cursor('SELECT 1+1 AS sum2;')
        # ic(r)
        # r2 = db.select_rows_dict_cursor(
        #     pg_sql_queries["get_period_id"], 
        #     {'period_title': 'Дополнение 67'}
        # )
        # ic(r2)
        # ic(list(r2[0].keys())) 
        # ic(type(r2[0]))  # 'period_id'
        # ic(r2[0]['period_id'])

        

        # query = pg_sql_queries["get_period_id"]
        # query_parameter = {'period_title': 'Дополнение 67'}
        # query_to_csv(db, 'test.csv', query, query_parameter)

        query = pg_sql_queries["get_group_work_process_for_period_id"]
        query_parameter = {'period_id': 153}
        r = query_to_csv(db, 'test.csv', query, query_parameter)
        ic(r)
