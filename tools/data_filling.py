import pandas as pd
from icecream import ic

from config import dbTolls, src_catalog_items
from sql_queries import sql_directory_creates
from tools.transfer_raw_catalog import transfer_raw_table_data_to_catalog


def read_csv_to_raw_table(db_file_name: str, csv_file_name: str, period: int):
    """ Читает данные из csv файла в df. Добавляет столбец 'период'.
     Записывает df в таблицу tblRawDat БД. """
    try:
        df = pd.read_csv(csv_file_name, delimiter=";", index_col=False, encoding="utf8", dtype=pd.StringDtype())
        df['PERIOD'] = period
        memory = f"использовано памяти: {df.memory_usage(index=True, deep=True).sum():_} bytes"
        ic(memory)
        with dbTolls(db_file_name) as db:
            table_name = f"tblRawData"
            db.go_execute(f"DROP TABLE IF EXISTS {table_name};")
            df.to_sql(table_name, db.connection, if_exists='append', index=False)
            db.connection.commit()
            x = pd.read_sql_query(sql=f"SELECT COUNT(rowid) AS count FROM {table_name};", con=db.connection)
            count = f"вставлено записей {int(x.iloc[0]['count'])} в таблицу {table_name}"
            ic(count)
    except IOError as err:
        print(err, csv_file_name)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"

    catalog_data = os.path.join(data_path, "TABLES_67.csv")
    quotes_data = os.path.join(data_path, "WORK_PROCESS_67.csv")
    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    ic(db_name)
    period = 67

    # ic(catalog_data)
    # read_csv_to_raw_table(db_name, catalog_data, period)

    ic(quotes_data)
    read_csv_to_raw_table(db_name, quotes_data, period)
