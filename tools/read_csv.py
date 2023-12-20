import pandas as pd
from icecream import ic
from pathlib import Path
from config import dbTolls

from files_features import output_message_exit


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

            count = f"{int(x.iloc[0]['count'])} записей вставлено в таблицу {table_name} из файла {Path(csv_file_name).name}"
            ic(count)
    except IOError as err:
        output_message_exit(err, csv_file_name)


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
