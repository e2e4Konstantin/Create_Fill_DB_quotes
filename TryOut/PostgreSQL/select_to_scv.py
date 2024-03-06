import os
import pandas as pd
from sqlalchemy import create_engine, select, text
from sqlalchemy import MetaData, Table
from sqlalchemy import inspect
from pathlib import Path

CONF = 'postgresql+psycopg2://read_larix:read_larix@192.168.23.3:5432/normativ'

def test_df():
    engine = create_engine(CONF)
    inspector = inspect(engine)
    # schemas = inspector.get_schema_names()
    # for schema in schemas:
    #     print(f"schema: {schema!r}")
        # for table_name in inspector.get_table_names(schema=schema):
        #     print(f"table: {table_name}")
            # for column in inspector.get_columns(table_name, schema=schema):
            #     print(f"Column: {column}")
    for table_name in inspector.get_table_names(schema='larix'):
            print(f"table: {table_name!r}")

    
    # queries = [
    #     "SELECT * FROM larix.period",
    #     "SELECT * from larix.period AS p ORDER BY p.date_start DESC"
    # ]
    # conn = engine.connect()
    # df = pd.read_sql_query(sql=text(queries[1]), con=conn)
    # print(df)

    # stmt = select([
    #     table.columns.column1,
    #     table.columns.column2
    # ]).where(
    #     (table.columns.column1 == 'filter1')
    #     &
    #     (table.columns.column2 == 'filter2')
    # )

    # connection = engine.connect()
    # results = connection.execute(stmt).fetchall()


def convert_to_csv(tablename, filename):
    engine = create_engine(CONF)
    connection = engine.connect()

    metadata = MetaData(schema='larix')
    table = Table(tablename, metadata, autoload_with=engine)
    stmt = select(table)
    results = connection.execute(stmt).fetchall()  # .fetchmany(size=10)

    filepath = Path(filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(results)
    df.to_csv(filepath, index=False)

    print(f'\n🆗 exported successfully into {os.getcwd()}/{filepath}\n')

convert_to_csv('period', './your-new-file.csv')

# test_df()
