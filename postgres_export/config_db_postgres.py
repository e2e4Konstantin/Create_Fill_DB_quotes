
import psycopg2
from psycopg2.extras import DictCursor
from contextlib import closing
import traceback
from typing import Optional, Type
from types import TracebackType

from postgres_export.pg_config import AccessData, db_access

class PostgresDB:
    """PostgreSQL class."""
    def __init__(self, data: AccessData ):
        self.host = data.host
        self.username = data.user
        self.password = data.password
        self.port = data.port
        self.dbname = data.dbname
        self.connection = None
        self.cursor = None
        self.LIMIT = 10

    def connect(self):
        """Connect to a database."""
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                traceback.print_exc(e)
                # print(e)
                raise e
            finally:
                print(f"Соединение с БД {self.dbname!r} установлено.")

    def __enter__(self):
        self.connect()
        if self.connection:
            self.cursor = self.connection.cursor(cursor_factory=DictCursor)
            return self
            # return (self.connection, self.cursor)
        # return (None, None)
        return None

    def __exit__(
            self, 
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
        ):
        if exc_val:
            traceback.print_exc()
            print('произошла непредвиденная ошибка при закрытии БД')
            raise exc_val
        if self.cursor:
            try:
                self.cursor.close() 
            except psycopg2.DatabaseError as e:
                traceback.print_exc(e)
                raise e
        if self.connection:
            try:
                self.connection.close()
            except psycopg2.DatabaseError as e:
                traceback.print_exc(e)
                raise e
        self.cursor, self.connection = None, None


    def update_rows(self, query) -> str | None:
        """ update rows """
        self.connect()
        if self.connect:
            with closing(self.connect) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
            return f"{cur.rowcount} rows affected."
        return None    
        

    def select_rows_dict_cursor(self, query, args=None):
        """ SELECT as dicts."""
        self.connect()
        # with closing(self.connection) as conn:
        with self.connection as conn:    
            try:
                self.cursor.execute(query, args)
                records = self.cursor.fetchall()
            except psycopg2.DatabaseError as e:
                traceback.print_exc(e)
                raise e
        return records if records else None



if __name__ == "__main__":
    from icecream import ic

    with PostgresDB(db_access['vlad']) as db:
        ic(db.connection)
        ic(db.cursor.execute('SELECT 1+1;'))
        sql = "COPY (SELECT * FROM larix.period p LIMIT 10) TO STDOUT WITH CSV DELIMITER ';';"

        with open("test.csv", "w", encoding='utf-8') as file:
            db.cursor.copy_expert(sql, file)
