import pandas as pd
from icecream import ic

from files_features import create_abspath_file
from config import dbTolls


from files_features import output_message_exit


def _read_excel_to_df(excel_file: str, sheet_name: str) -> pd.DataFrame | None:
    """Читает данные из XLS файла в DataFrame"""
    try:
        df = pd.read_excel(
            excel_file, sheet_name=sheet_name, dtype="object", engine="openpyxl"
        )
        if df.empty:
            output_message_exit(
                f"Данные из файла {excel_file!r}", "Не прочитались."
            )
        return df
    except IOError as err:
        output_message_exit("Ошибка при чтении файла", f"{excel_file!r}\n{err}")


def _read_csv_to_df(csv_file_name: str, delimiter: str = ";") -> pd.DataFrame | None:
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
                f"Данные из файла {csv_file_name!r}", "Не прочитались."
            )
        return df
    except IOError as err:
        output_message_exit(err, csv_file_name)


def _load_df_to_db_table(df: pd.DataFrame, db_file: str, table_name: str) -> int:
    """Загружает данные из df в таблицу table_name базы данных db_file"""
    with dbTolls(db_file) as db:
        df.to_sql(
            name=table_name,
            con=db.connection,
            if_exists="replace",
            index=False,
            chunksize=100,
            method="multi",
        )  # dtype=pandas.StringDtype(),

        # count = db.go_select(f"SELECT COUNT() AS count FROM {table_name};")[0]["count"]
        # message = f"Из df импортировано: {count} записей в {table_name!r}"
        # ic(message)
    return 0


def load_csv_to_raw_table(csv_path: str, db_path: str, delimiter: str = ";") -> int:
    """Imports data from a CSV file to the tblRawData table."""
    df = pd.read_csv(csv_path, delimiter=delimiter, index_col=False, dtype=str)
    # df.to_clipboard()
    return _load_df_to_db_table(df, db_path, "tblRawData")


def load_xlsx_to_raw_table(excel_file: str, sheet_name: str, db_file: str) -> int:
    """Заполняет таблицу tblRawData данными из excel файла со страницы sheet_name."""
    df: pd.DataFrame = _read_excel_to_df(excel_file, sheet_name)
    # df.to_clipboard()
    return _load_df_to_db_table(df, db_file, "tblRawData")
