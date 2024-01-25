import os
from icecream import ic
from config import work_place
from tools import read_csv_to_raw_table, transfer_raw_data_to_options
#
# from parameter_extraction import create_tables_idx_options, transfer_raw_table_to_options
# from tools import read_csv_to_raw_table

if __name__ == '__main__':
    now = "office"  # office  # home
    db_name, _, data_path = work_place(now)
    options_data = [
        "options_Материалы_1_13.csv",
        "options_Расценки_3_68.csv",
        "options_Расценки_4_68.csv",
        "options_Расценки_5_67.csv",
        "options_Расценки_6_68.csv",
    ]

    split_data = os.path.join(data_path, options_data[0])
    ic(db_name)
    ic(split_data)

    # # ! Удаляем таблицы для атрибутов и создаем новый
    # create_tables_idx_options(db_name)

    # прочитать из csv файла данные для Параметров в таблицу tblRawData для периода period
    read_csv_to_raw_table(db_name, split_data, period)

    # заполнить Параметры данными из таблицы tblRawData
    transfer_raw_table_to_options(db_name)
