from config.file_location_config import FileLocation
from config.const import LOCATIONS

location = "office"
data_local_paths = {"office": r"c:\bin", "home": r"f:\tmp"}
path_local = data_local_paths[location]

catalog_data = FileLocation(file_name="TABLES_67.csv", path=path_local)
quotes_data = FileLocation("WORK_PROCESS_67.csv", path_local)
materials_data = FileLocation("1_глава_67_доп.csv", path_local)
machines_data = FileLocation("2_глава_67_доп.csv", path_local)
equipments_data = FileLocation("13_глава_34_доп.csv", path_local)
pnwc_catalog = FileLocation(
    "Каталог_НЦКР_Временный_каталог_Март_2022_Ресурсы_ТСН.csv", path_local)
pnwc_resource = FileLocation(
    "Данные_НЦКР_Временный_каталог_НЦКР_2023_4_кв.csv", path_local)




# db_name, data_path, param_path = work_place(now)

# # period = 67


class SrcData:
    """ Класс для исходных данных под загрузку. """
    places = LOCATIONS
    period_types = ('supplement', 'index')
    period_holders = ('ton', 'equipments', 'monitoring')

    def __init__(self, place_name: str, db_file_name: str, db_path: str):
        self.place_name = place_name
        self.db_file_name = db_file_name
        self.db_path = {places[0]: "", places[1]: ""}
        self.data_path = {places[0]: "", places[1]: ""}

        self.catalog_data = DataFile()
        self.period_data = DataFile()
        self.quotes_data = DataFile()
        self.materials_data = DataFile()
        self.machines_data = DataFile()
        self.equipments_data = DataFile()
        self.pnwc_catalog = DataFile()
        self.pnwc_resource = DataFile()


if __name__ == '__main__':
    from icecream import ic
    location = "office"
    local_paths = LocationPath("", "")

    # location_db = SrcDBLocation(location, "Normative.sqlite3", local_paths)

#     src_data = SrcData(location, "Normative.sqlite3")
#     src_data.db_path["office"] = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
#     src_data.db_path["home"] = r""

#     src_data.period_data.file_name = "periods.xlsx"
#     src_data.period_data.path_name = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Периоды"

#     src_data.catalog_data.file_name = "TABLES_67.csv"
#     src_data.catalog_data.path_name = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\csv"
#     src_data.catalog_data.period_holder = "ton"
#     src_data.catalog_data.period_type = "supplement"

import pathlib
from collections import namedtuple
from const import LOCATIONS
from files_features import create_abspath_file


LocationPath = namedtuple(typename="LocationPath", field_names=[*LOCATIONS])


class SrcDBLocation:
    """ Класс для места расположения Базы данных. """

    def __init__(self, place_name: str, db_file_name: str, paths: tuple[str, str] = None):
        self.place_name = place_name
        self.db_file_name = db_file_name
        self.db_file = {x[0]: create_abspath_file(
            x[1], self.db_file_name) for x in zip(LOCATIONS, paths)}

    @classmethod
    def check_db_exists() -> bool:
        return pathlib.Path.exists(cls.db_file[self.place_name])


# if __name__ == '__main__':
#     # from icecream import ic
#     location = "office"
#     local_paths = LocationPath()
