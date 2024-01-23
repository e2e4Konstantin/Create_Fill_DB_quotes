import os
from collections import namedtuple

PlacePath = namedtuple("PlacePath", ["db_file", "data_path", "param_path"])


def work_place(point_name: str) -> PlacePath:
    """ Формирует ссылки на данные в зависимости от места запуска. """
    db_name = "Normative.sqlite3"
    places = {
        "office": PlacePath(
            db_file=os.path.join(r"C:\Users\kazak.ke\Documents\PythonProjects\DB", db_name),
            data_path=r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\csv",
            param_path=r"C:\Users\kazak.ke\Documents\Задачи\Парсинг_параметризация\csv"
        ),
        "home": PlacePath(
            db_file=os.path.join(r"F:\Kazak\GoogleDrive\Python_projects\DB", db_name),
            data_path=r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv",
            param_path=r"F:\Kazak\GoogleDrive\NIAC\parameterisation\Split\csv"
        ),
    }
    return places[point_name]


if __name__ == '__main__':
    now = "office"  # office  # home
    _, data, param = work_place(now)
    print( data, param)
    print(work_place(now))
    print(work_place(now).db_file)
