import pandas as pd
from icecream import ic

from files_features import create_abspath_file
from excel_features import read_excel_to_df
from config import dbTolls


from files_features import output_message_exit


def read_excel_to_df(excel_file: str, sheet_name: str) -> pd.DataFrame | None:
    """Читает данные из XLS файла в DataFrame"""
    try:
        df = pd.read_excel(
            excel_file, sheet_name=sheet_name, dtype="object", engine="openpyxl"
        )
        if df.empty:
            output_message_exit(
                f"Данные из файла {excel_file_name!r}", f"Не прочитались."
            )
        return df
    except IOError as err:
        output_message_exit(f"Ошибка при чтении файла", f"{excel_file_name!r}")


def read_csv_to_df(csv_file_name: str, delimiter: str = ";") -> pd.DataFrame | None:
    """Читает данные из CSV файла в df."""
    try:
        df = pd.read_csv(
            csv_file_name,
            delimiter=delimiter,
            index_col=False,
            encoding="utf8",
            dtype=pd.StringDtype(),
        )
        if df.empty:
            output_message_exit(
                f"Данные из файла {excel_file_name!r}", f"Не прочитались."
            )
        return df
    except IOError as err:
        output_message_exit(err, csv_file_name)




def load_df_to_db_table(df: pd.DataFrame, db_file: str, table_name: str) -> int:
    """Загружает данные из df в таблицу table_name базы данных db_file"""
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




def load_csv_to_raw_table(csv_file: str, db_file: str, delimiter: str = ";") -> int:
    """Заполняет таблицу tblRawData данными из csv файла. """
    df: DataFrame = read_csv_to_df(csv_file_name, delimiter)
    # df.to_clipboard()
    result = load_df_to_db_table(df, db_full_file_name, "tblRawData")
    return result
