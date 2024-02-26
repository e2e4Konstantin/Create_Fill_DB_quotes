
from pandas import DataFrame

from files_features import create_abspath_file
from excel_features import read_excel_to_df
from config import dbTolls


def load_df_to_db_table(data: DataFrame, db_file_name: str=None, table_name: str=None) -> int:
    """Загружает данные из df в таблицу table_name базы данных db_file_name """
    with dbTolls(db_file_name) as db:
        data.to_sql(name=table_name, con=db.connection, if_exists='replace', index=False, method='multi') #dtype=pandas.StringDtype(),
        count = db.go_select(F"SELECT COUNT() AS count FROM {table_name};")[0]['count']
        print(f"Из df импортировано: {count} записей в {table_name!r}")
    return 0
