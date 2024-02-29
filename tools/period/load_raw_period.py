
from pandas import DataFrame

from excel_features import read_excel_to_df
from tools import load_df_to_db_table


def _load_raw_data_periods(excel_file_name: str, sheet_name: str, db_full_file_name: str) -> int:
    """ Заполняет таблицу tblRawData данными из excel файла со страницы sheet_name данными выгрузки периодов. """
    df: DataFrame = read_excel_to_df(excel_file_name, sheet_name)
    # df.to_clipboard()
    return load_df_to_db_table(df, db_full_file_name, "tblRawData")






if __name__ == '__main__':
    from files_features import create_abspath_file

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_name = "Normative.sqlite3"
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\Периоды"
    file, sheet = "periods.xlsx", "Sheet"

    db_name_full = create_abspath_file(db_path, db_name)
    excel_name_full = create_abspath_file(data_path, file)
    print("Код возврата:", _load_raw_periods(excel_name_full, sheet, db_name_full))
