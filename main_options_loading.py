import os
from icecream import ic

from parameter_extraction import create_tables_idx_options, transfer_raw_table_to_options
from tools import read_csv_to_raw_table

if __name__ == '__main__':
    data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    period = 68
    options_data = [
        "Расценки_3_68_split_options.csv",
        "Расценки_4_68_split_options.csv",
        "Расценки_5_68_split_options.csv",
        "Расценки_6_68_split_options.csv",
        "Расценки_10_68_split_options.csv",
    ]

    split_data = os.path.join(data_path, options_data[4])

    ic(db_name)
    ic(split_data)

    # # ! Удаляем таблицы для атрибутов и создаем новый
    # create_tables_idx_options(db_name)

    # прочитать из csv файла данные для Параметров в таблицу tblRawData для периода period
    read_csv_to_raw_table(db_name, split_data, period)

    # заполнить Параметры данными из таблицы tblRawData
    transfer_raw_table_to_options(db_name)
