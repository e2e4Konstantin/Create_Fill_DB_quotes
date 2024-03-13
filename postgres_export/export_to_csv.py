
from icecream import ic

from psycopg2 import sql

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.config_db_postgres import PostgresDB


def query_to_csv(csv_file_name: str, query: str, args=None):
    results = db.select_rows_dict_cursor(query, args)
    df = pd.DataFrame(results)
    df.to_csv(csv_file_name, index=False)




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

        query_to_csv('test.csv', pg_sql_queries["get_period_id"],
                     {'period_title': 'Дополнение 67'})
        
