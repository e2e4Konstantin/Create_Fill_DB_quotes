import os
from icecream import ic
from config import work_place
from tools import read_csv_to_raw_table, transfer_raw_data_to_options


if __name__ == '__main__':
    """ Читает исходные данные с атрибутами из *.csv файла в tblRawData. 
        Ищет владельца атрибута, записывает атрибут из таблицы tblRawData в рабочую таблицу атрибутов. 
        Период контролируется только у ТСН каталога."""

    now = "home"  # office  # home
    db_name, _, data_path = work_place(now)
    options_data = [
        "Options_Материалы_1_13_split.csv",
        "Options_Машины_2_68_split.csv",
        "Options_Оборудование_13_68_split.csv",
        #
        "Options_Расценки_3_68_split.csv",
        "Options_Расценки_4_68_split.csv",
        "Options_Расценки_5_67_split.csv",
        "Options_Расценки_6_68_split.csv",
        "Options_Расценки_10_68_split.csv",

    ]

    period = 68
    for data_file in options_data:
        split_data = os.path.join(data_path, data_file)
        ic(db_name, split_data)
        read_csv_to_raw_table(db_name, split_data, set_period=period)
        transfer_raw_data_to_options(db_name)
