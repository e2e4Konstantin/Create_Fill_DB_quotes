from pandas import DataFrame
from icecream import ic
from files_features import create_abspath_file
from excel_features import read_excel_to_df
from config import dbTolls


def load_df_to_db_table(
    df: DataFrame, db_file: str = None, table_name: str = None
) -> int:
    """Загружает данные из df в таблицу table_name базы данных db_file_name"""
    with dbTolls(db_file) as db:
        df.to_sql(
            name=table_name,
            con=db.connection,
            if_exists="replace",
            index=False,
            method="multi",
        )  # dtype=pandas.StringDtype(),

        count = db.go_select(f"SELECT COUNT() AS count FROM {table_name};")[0]["count"]
        message = f"Из df импортировано: {count} записей в {table_name!r}"
        ic(message)
    return 0
