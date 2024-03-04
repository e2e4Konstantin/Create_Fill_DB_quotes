
from config.file_location_config import FileLocation, Period, FileDataInfo
from config.const import LOCATIONS


location = "office"
data_local_paths = {"office": r"c:\bin", "home": r"f:\tmp"}
path_local = data_local_paths[location]

# 68
tsn_period_68_dop = Period("тсн", "дополнение", 68, 196)
eqp_period_35_dop = Period("оборудование", "дополнение", 35, 3)

# 67
tsn_period_67_dop = Period("тсн", "дополнение", 67, 195)
eqp_period_34_dop = Period("оборудование", "дополнение", 34, 40)


src_data = {
    "catalog_68_dop": FileDataInfo(FileLocation("TABLES_68.csv", path_local), tsn_period_67_dop),
    "quotes_68_dop":  FileDataInfo(FileLocation("WORK_PROCESS_68.csv", path_local), tsn_period_67_dop),
    "materials_68_dop": FileDataInfo(FileLocation("1_глава_68_доп.csv", path_local), tsn_period_67_dop),
    "machines_68_dop": FileDataInfo(FileLocation("2_глава_68_доп.csv", path_local), tsn_period_67_dop),
    "equipments_35_dop": FileDataInfo(FileLocation("13_глава_35_доп.csv", path_local), eqp_period_34_dop),

    "catalog_67_dop": FileDataInfo(FileLocation("TABLES_67.csv", path_local), tsn_period_67_dop),
    "quotes_67_dop":  FileDataInfo(FileLocation("WORK_PROCESS_67.csv", path_local), tsn_period_67_dop),
    "materials_67_dop": FileDataInfo(FileLocation("1_глава_67_доп.csv", path_local), tsn_period_67_dop),
    "machines_67_dop": FileDataInfo(FileLocation("2_глава_67_доп.csv", path_local), tsn_period_67_dop),
    "equipments_34_dop": FileDataInfo(FileLocation("13_глава_34_доп.csv", path_local), eqp_period_34_dop),
    "pnwc_catalog_67_dop": FileDataInfo(FileLocation("Каталог_НЦКР_Временный_каталог_Март_2022_Ресурсы_ТСН.csv", path_local), tsn_period_67_dop),
    "pnwc_resource_67_dop": FileDataInfo(FileLocation("Данные_НЦКР_Временный_каталог_НЦКР_2023_4_кв.csv", path_local), tsn_period_67_dop),
}
