import pandas as pd
from icecream import ic
from pathlib import Path
from config import dbTolls

from tools.shared.read_excel_files import read_csv_to_df
from tools.shared.load_df_to_db_table import load_df_to_db_table
from files_features import output_message_exit


def read_csv_with_period_to_raw_table(
    db_file: str, csv_file: str, period_number: int, new_columns: list[str] = None
) -> None:
    """Читает данные из csv файла в df.  Добавляет в df столбцы new_columns.
    Добавляет и инициализирует столбец 'PERIOD'. Записывает df в таблицу tblRawData."""
    try:
        df = read_csv_to_df(csv_file_name)
        if new_column_name is not None:
            df.columns = (
                new_column_name + df.columns.to_numpy().tolist()[len(new_column_name) :]
            )
        df["PERIOD"] = period_number
        load_df_to_db_table(df, db_file, table_name="tblRawData")
    except IOError as err:
        output_message_exit(err, csv_file_name)


if __name__ == "__main__":
    read_csv_to_raw_table(db_name, quotes_data, period)
