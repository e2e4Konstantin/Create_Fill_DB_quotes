# https://khashtamov.com/en/postgresql-with-python-and-psycopg2/
# https://stackoverflow.com/questions/68656663/creating-a-method-to-connect-to-postgres-database-in-python
# https://www.psycopg.org/psycopg3/docs/basic/adapt.html

# 
from psycopg2.extras import RealDictCursor
import traceback
from psycopg2.extras import DictCursor
from contextlib import closing
import psycopg2
conn = psycopg2.connect(dbname='database', user='db_user',
                        password='mypassword', host='localhost')
cursor = conn.cursor()

cursor.execute('SELECT * FROM airport LIMIT 10')
records = cursor.fetchall()
...
cursor.close()
conn.close()


cursor.fetchone() # returns a single row
cursor.fetchall() # returns a list of rows
cursor.fetchmany(size=5) # returns the provided number of rows

for row in cursor:
    print(row)


with closing(psycopg2.connect(...)) as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM airport LIMIT 5')
        for row in cursor:
            print(row)

# to get a value by column name you can use NamedTupleCursor or DictCursor:

with psycopg2.connect(...) as conn:
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        ...

cursor.execute('SELECT * FROM airport WHERE city_code = %s', ('ALA', ))
for row in cursor:
    print(row)

cursor.execute(
    'SELECT * FROM engine_airport WHERE city_code = %(city_code)s',
    {'city_code': 'ALA'}
)


from psycopg2 import sql

with conn.cursor() as cursor:
        columns = ('country_name_ru', 'airport_name_ru', 'city_code')

        stmt = sql.SQL('SELECT {} FROM {} LIMIT 5').format(
            sql.SQL(',').join(map(sql.Identifier, columns)),
            sql.Identifier('airport')
        )
        cursor.execute(stmt)

        for row in cursor:
            print(row)

with conn.cursor() as cursor:
    # conn.autocommit = True
    values = [
        ('ALA', 'Almaty', 'Kazakhstan'),
        ('TSE', 'Astana', 'Kazakhstan'),
        ('PDX', 'Portland', 'USA'),
    ]
    insert = sql.SQL('INSERT INTO city (code, name, country_name) VALUES {}').format(
        sql.SQL(',').join(map(sql.Literal, values))
    )
    cursor.execute(insert)

cur.execute("SELECT %s, %s", (True, False)) # equivalent to "SELECT true, false"

conn.info.timezone

zoneinfo.ZoneInfo(key='Europe/London')
conn.execute("select '2048-07-08 12:00'::timestamptz").fetchone()[0]
datetime.datetime(2048, 7, 8, 12, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/London'))


conn.execute("SET TIMEZONE to 'Europe/Rome'")  # UTC+2 in summer
# UTC input
conn.execute("SELECT '2042-07-01 12:00Z'::timestamptz").fetchone()[0]
datetime.datetime(2042, 7, 1, 14, 0,
                  tzinfo=zoneinfo.ZoneInfo(key='Europe/Rome'))


conn.execute("SELECT * FROM mytable WHERE id = ANY(%s)", [[10, 20, 30]])




# ------------------------------------------------------------


class Postgres(object):
    def __init__(self, *args, **kwargs):
        self.dbName = args[0] if len(args) > 0 else 'prod'
        self.args = args

    def _connect(self, msg=None):
        if self.dbName == 'dev':
            dsn = 'host=127.0.0.1 port=5556 user=xyz password=xyz dbname=development'
        else:
            dsn = 'host=127.0.0.1 port=5557 user=xyz password=xyz dbname=production'

        try:
            self.con = psycopg2.connect(dsn)
            self.cur = self.con.cursor(cursor_factory=RealDictCursor)
        except:
            traceback.print_exc()

    def __enter__(self, *args, **kwargs):
        self._connect()
        return (self.con, self.cur)

    def __exit__(self, *args):
        for c in ('cur', 'con'):
            try:
                obj = getattr(self, c)
                obj.close()
            except:
                pass  # handle it silently!?
        self.args, self.dbName = None, None


with Postgres('dev') as (con, cur):
    print(con)
    print(cur.execute('select 1+1'))
print(con)  # verify connection gets closed!




