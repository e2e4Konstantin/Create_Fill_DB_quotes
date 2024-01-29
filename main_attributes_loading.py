import os
from icecream import ic
from config import work_place
from tools import read_csv_to_raw_table, transfer_raw_data_to_attributes

if __name__ == '__main__':
    """ Читает исходные данные с атрибутами из *.csv файла в tblRawData. 
    Ищет владельца атрибута, записывает атрибут из таблицы tblRawData в рабочую таблицу атрибутов. 
    Период контролируется только у ТСН каталога."""

    now = "home"  # office  # home
    db_name, _, data_path = work_place(now)
    # ic(db_name, data_path)
    attributes_data = (
        "Расценки_3_68_split_attributes.csv",
        "Расценки_4_68_split_attributes.csv",
        "Расценки_5_68_split_attributes.csv",
        "Расценки_6_68_split_attributes.csv",
        "Расценки_10_68_split_attributes.csv",
        "Материалы_1_13_split_attributes.csv",
        "Машины_2_68_split_attributes.csv",
        "Оборудование_13_68_split_attributes.csv"
    )

    period = 68
    for data_file in attributes_data:
        split_data = os.path.join(data_path, data_file)
        ic(db_name, split_data)
        read_csv_to_raw_table(db_name, split_data, set_period=period)
        transfer_raw_data_to_attributes(db_name)
