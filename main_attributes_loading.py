import os
from icecream import ic
from config import work_place
from tools import read_csv_to_raw_table, transfer_raw_data_to_attributes

if __name__ == '__main__':
    now = "office"  # office  # home
    db_name, _, data_path = work_place(now)
    attributes_data = [
        "Расценки_3_68_split_attributes.csv",
        "Расценки_4_68_split_attributes.csv",
        "Расценки_5_68_split_attributes.csv",
        "Расценки_6_68_split_attributes.csv",
        "Расценки_10_68_split_attributes.csv",
    ]
    split_data = os.path.join(data_path, attributes_data[3])
    ic(db_name, split_data)
    period = 68
    # прочитать из csv файла данные Атрибутов в таблицу tblRawData для периода period
    # read_csv_to_raw_table(db_name, split_data, 68)
    #
    # заполнить Каталог данными из таблицы tblRawData
    transfer_raw_data_to_attributes(db_name)
