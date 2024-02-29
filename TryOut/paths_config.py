import pathlib
from collections import namedtuple
from const import LOCATIONS
from files_features import create_abspath_file


LocationPath = namedtuple(typename="LocationPath", field_names=[
                          *LOCATIONS], defaults=[None for _ in LOCATIONS])


class SrcDBLocation:
    """ Класс для места расположения Базы данных. """

    def __init__(self, place_name: str, db_file_name: str, paths: LocationPath):
        self.place_name = place_name
        self.db_file_name = db_file_name
        self.paths = paths
        self.db_paths = {key: create_abspath_file(value, self.db_file_name)
                        for key, value in self.paths._asdict().items()}
        self.db_location = self.db_paths[self.place_name]

    def __str__(self) -> str:
        return self.db_location
    
    @classmethod
    def check_db_exists(cls) -> bool:
        return pathlib.Path.exists(cls.db_location)


if __name__ == '__main__':
    # from icecream import ic
    location = "office"

    local_paths = LocationPath(office=r"c:\bin", home=r"f:\tmp")
    print(local_paths.location)
    db_location = SrcDBLocation(
        place_name=location, db_file_name="Normative.sqlite3", paths=local_paths
    )
    print(db_location)
    print(db_location.check_db_exists())
    print(db_location.db_paths)
    print(db_location.db_location)

