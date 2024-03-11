

from config.file_location_config import FileLocation, Period, LocalPath, LocalData, FileDataInfo
from config.const import LOCATIONS
from files_features import create_abspath_file


def set_data_location(work_location_name: str) -> LocalData:
    """ Формирует ссылки на данные в зависимости от места работы. """
    db_file_name = "Normative.sqlite3"
    src_periods_file_name = "period_export_table.csv"  # "periods.csv"
    local_paths = {
        "office": LocalPath(
            db_file=create_abspath_file(
                r"C:\Users\kazak.ke\Documents\PythonProjects\DB", db_file_name),
            data_path=r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\csv",
            param_path=r"C:\Users\kazak.ke\Documents\Задачи\Парсинг_параметризация\csv",
            periods_path=create_abspath_file(
                r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\output", src_periods_file_name)
        ),
        "home": LocalPath(
            db_file=create_abspath_file(
                r"F:\Kazak\GoogleDrive\Python_projects\DB", db_file_name),
            data_path=r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv",
            param_path=r"F:\Kazak\GoogleDrive\NIAC\Задачи\4_параметризация\SRC\Split\csv",
            periods_path=create_abspath_file(
                r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\Периоды", src_periods_file_name)
        ),
    }
    data_local_path = local_paths[work_location_name].data_path

    data = LocalData()
    data.db_file = local_paths[work_location_name].db_file
    data.src_periods_data = local_paths[work_location_name].periods_path

    # 67 Period
    data.periods["tsn_period_67_dop"] = Period("тсн", "дополнение", 67, 195)
    data.periods["eqp_period_34_dop"] = Period("оборудование", "дополнение", 34, 40)
    # 68 Period
    data.periods["tsn_period_68_dop"] = Period("тсн", "дополнение", 68, 196)
    data.periods["eqp_period_35_dop"] = Period("оборудование", "дополнение", 35, 3)

    # 67 data
    data.src_data["catalog_67_dop"] = FileDataInfo(FileLocation(
        "TABLES_67.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["quotes_67_dop"] = FileDataInfo(FileLocation(
        "WORK_PROCESS_67.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["materials_67_dop"] = FileDataInfo(FileLocation(
        "1_глава_67_доп.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["machines_67_dop"] = FileDataInfo(FileLocation(
        "2_глава_67_доп.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["equipments_34_dop"] = FileDataInfo(FileLocation(
        "13_глава_34_доп.csv", data_local_path), data.periods["eqp_period_34_dop"])
    data.src_data["pnwc_catalog_67_dop"] = FileDataInfo(FileLocation(
        "Каталог_НЦКР_Временный_каталог_Март_2022_Ресурсы_ТСН.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["pnwc_resource_67_dop"] = FileDataInfo(FileLocation(
        "Данные_НЦКР_Временный_каталог_НЦКР_2023_4_кв.csv", data_local_path), data.periods["tsn_period_67_dop"])
    # 68 data
    data.src_data["catalog_68_dop"] = FileDataInfo(FileLocation(
        "TABLES_68.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["quotes_68_dop"] = FileDataInfo(FileLocation(
        "WORK_PROCESS_68.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["materials_68_dop"] = FileDataInfo(FileLocation(
        "1_глава_68_доп.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["machines_68_dop"] = FileDataInfo(FileLocation(
        "2_глава_68_доп.csv", data_local_path), data.periods["tsn_period_67_dop"])
    data.src_data["equipments_35_dop"] = FileDataInfo(FileLocation(
        "13_глава_35_доп.csv", data_local_path), data.periods["eqp_period_34_dop"])


    return data


if __name__ == "__main__":
    location = "home"
    di = set_data_location(location)
    print(di)
    print(di.periods.keys())
    print(di.src_data.keys())
