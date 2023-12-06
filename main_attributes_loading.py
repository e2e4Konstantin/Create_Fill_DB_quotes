import os
from icecream import ic

from parameter_extraction import create_tables_idx_attributes, transfer_raw_table_to_attributes
from tools import read_csv_to_raw_table

if __name__ == '__main__':
    data_path = r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"

    db_name = os.path.join(db_path, "quotes_test.sqlite3")

    period = 68
    attributes_data = [
        "Расценки_3_68_split_attributes.csv",
        "Расценки_4_68_split_attributes.csv",
        "Расценки_5_68_split_attributes.csv",
        "Расценки_6_68_split_attributes.csv",
        "Расценки_10_68_split_attributes.csv",
    ]

    split_data = os.path.join(data_path, attributes_data[3])

    ic(db_name)
    ic(split_data)

    # # !! Удаляем таблицы и индексы для атрибутов и создаем новый
    # create_tables_idx_attributes(db_name)

    # прочитать из csv файла данные Атрибутов в таблицу tblRawData для периода period
    read_csv_to_raw_table(db_name, split_data, period)

    # заполнить Каталог данными из таблицы tblRawData
    transfer_raw_table_to_attributes(db_name)
