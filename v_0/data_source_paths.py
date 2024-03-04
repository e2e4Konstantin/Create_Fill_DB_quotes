from icecream import ic
from config import work_place



def set_data_source_paths():
    """Установка источников данных для загрузки """
    db_name, data_path, param_path = work_place(now)

    # period = 67
    # catalog_data = os.path.join(data_path, "TABLES_67.csv")
    # quotes_data = os.path.join(data_path, "WORK_PROCESS_67.csv")
    # materials_data = os.path.join(data_path, "1_глава_67_доп.csv")
    # machines_data = os.path.join(data_path, "2_глава_67_доп.csv")
    # equipments_data = os.path.join(data_path, "13_глава_34_доп.csv")
    # pnwc_catalog = os.path.join(data_path, "Каталог_НЦКР_Временный_каталог_Март_2022_Ресурсы_ТСН.csv")
    # pnwc_resource = os.path.join(data_path, "Данные_НЦКР_Временный_каталог_НЦКР_2023_4_кв.csv")

    period = 68
    catalog_data = os.path.join(data_path, "TABLES_68.csv")
    quotes_data = os.path.join(data_path, "WORK_PROCESS_68.csv")
    materials_data = os.path.join(data_path, "1_глава_68_доп.csv")
    machines_data = os.path.join(data_path, "2_глава_68_доп.csv")
    equipments_data = os.path.join(data_path, "13_глава_35_доп.csv")

    ic(version, db_name, period)
