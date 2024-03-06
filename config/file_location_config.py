import pathlib
from collections import namedtuple

from files_features import create_abspath_file


class FileLocation:
    """Класс для места расположения Базы данных."""

    def __init__(self, file_name: str, path: str):
        self.file_name = file_name
        self.path = path
        self.location = create_abspath_file(self.path, self.file_name)
        self.exists = self.check_file_exists()

    def __str__(self) -> str:
        return f"file: {self.location!r} exists: {self.exists}"

    def check_file_exists(self) -> bool:
        return pathlib.Path(self.location).exists()


class Period:
    """Класс для ввода периодов."""

    holders = {"тсн": "ton", "оборудование": "equipments",
               "мониторинг": "monitoring"}
    categories = {
        "дополнение": "supplement",
        "индекс": "index",
        "специальный": "special",
    }

    def __init__(
        self, holder: str, category: str, supplement_number: int, index_number: int
    ):
        self.holder = Period.holders[holder]  # владелец периодов
        self.category = Period.categories[category]  # тип периода
        self.supplement_number = supplement_number  # номер дополнения
        self.index_number = index_number  # номер индекса

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: раздел: {self.holder!r} категория: {self.category} дополнение: {self.supplement_number} индекс: {self.index_number}"

    def get_data(self) -> tuple[str, str, str, str]:
        return self.holder, self.category, self.supplement_number, self.index_number




FileDataInfo = namedtuple(typename="FileDataInfo", field_names=[
                          'file_location', 'period'])
FileDataInfo.__annotations__ = {'location': FileLocation, 'period': Period}

LocalPath = namedtuple(
    "LocalPath", ["db_file", "data_path", "param_path", "periods_path"])


class LocalData:
    db_file: str = None
    periods: dict[str: Period] = {}
    src_data: dict[str: FileDataInfo] = {} # src_data['catalog_68_dop'].file_location
    src_periods_data: str = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:\nБД: {self.db_file!r},\nпериоды: {len(self.periods)}, данные: {len(self.src_data)},\nисходные периоды:{self.src_periods_data!r}"


    def get_data(self, data_name: str) -> tuple[str, tuple[str, str, str, str]] | None:
        """Создает кортеж с полным именем файла и данными периода. """
        file = self.src_data[data_name].file_location.location
        period = self.src_data[data_name].period.get_data()
        return file, period


if __name__ == "__main__":
    location = "office"
    db_local_paths = {"office": r"c:\bin", "home": r"f:\tmp"}
    db_location = FileLocation(
        file_name="Normative.sqlite3", path=db_local_paths[location]
    )
    print(db_location)
    print(db_location.exists)
    print(db_location)

    p1 = Period("тсн", "дополнение", 71, 207)
    p2 = Period("тсн", "индекс", 70, 207)
    p3 = Period("тсн", "индекс", 70, 206)
    print(p1)
    p4 = Period("оборудование", "индекс", 38, 53)
    p5 = Period("оборудование", "дополнение", 38, 52)
    p6 = Period("оборудование", "индекс", 37, 51)

    p6 = Period("мониторинг", "индекс", 71, 208)
    p6 = Period("мониторинг", "индекс", 70, 207)
    p6 = Period("мониторинг", "индекс", 70, 206)
