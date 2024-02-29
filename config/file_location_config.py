import pathlib
from files_features import create_abspath_file

class FileLocation:
    """ Класс для места расположения Базы данных. """

    def __init__(self, file_name: str, path: str):
        self.file_name = file_name
        self.path = path
        self.location = create_abspath_file(self.path, self.file_name)
        self.exists = self.check_file_exists()

    def __str__(self) -> str:
        return f"file: {self.location!r} exists: {self.exists}"
    
    def check_file_exists(self) -> bool:
        return pathlib.Path(self.location).exists()


if __name__ == '__main__':
    location = "office"
    db_local_paths = {"office": r"c:\bin", "home": r"f:\tmp"}
    db_location = FileLocation(file_name="Normative.sqlite3", path=db_local_paths[location])
    print(db_location)
    print(db_location.exists)
    print(db_location)
    