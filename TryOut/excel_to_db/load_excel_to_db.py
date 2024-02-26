
# https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder

from pandas import DataFrame

from files_features import create_abspath_file
from excel_features import read_excel_to_df
from config import dbTolls
from tools import load_df_to_db_table


if __name__ == '__main__':
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_name = "Normative.sqlite3"
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\Периоды"
    file, sheet = "periods.xlsx", "Sheet"

    db_name_full = create_abspath_file(db_path, db_name)
    file_name_full = create_abspath_file(data_path, file)
    df = read_excel_to_df(file_name_full, sheet) # d.to_clipboard()
    print(df)

    r = load_df_to_db_table(df, db_name_full, "tblPeriodRaw")
