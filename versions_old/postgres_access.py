from sqlalchemy import create_engine, text
# install psycopg2
import pandas as pd




DIALECT = 'postgresql'
SQL_DRIVER = 'psycopg2'
USERNAME = 'read_larix'  # enter your username
PASSWORD = 'read_larix'  # enter your password
HOST = '192.168.23.3'  # enter the oracle db host url
PORT = 5432  # enter the oracle port number
SERVICE = 'normativ'  # enter the oracle db service name
# postgresql+psycopg2://read_larix:read_larix@192.168.23.3:5432/normativ
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(PORT) + '/' + SERVICE

engine = create_engine(ENGINE_PATH_WIN_AUTH)


queries = [
    "SELECT * FROM larix.period",
    "SELECT * from larix.period AS p ORDER BY p.date_start DESC"
]

conn = engine.connect()
df_periods_index = pd.read_sql_query(
    sql=text(queries[1]), con=conn)


print(df_periods_index)
