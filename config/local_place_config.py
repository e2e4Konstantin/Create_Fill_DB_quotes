from typing import NamedTuple

from config.const import CONFIG_FILE_NAME, DB_FILE_NAME
from files_features import create_abspath_file


data_path_config = {
    'config_file_name':         CONFIG_FILE_NAME,
    'db_file_name':             DB_FILE_NAME,
    'office': {
        'db_path':              r"C:\Users\kazak.ke\Documents\PythonProjects\DB",
        'periods_path':         r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Periods",
        'quote_catalog_path':   r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesCatalog",
        'quote_data_path':      r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesData"
    },
    'home': {
        'db_path':              r"F:\Kazak\GoogleDrive\Python_projects\DB",
    },
    'periods': {},
}


class LocalData(NamedTuple):
    config_file: str
    db_file: str
    periods_path: str
    quote_catalog_path: str
    quote_data_path: str


def get_data_location(location_name: str) -> LocalData:
    """ Формирует ссылки на данные в зависимости от места работы. """
    config_data = read_json_config(CONFIG_FILE_NAME)
    # place = LocalData()
    

if __name__ == "__main__":
    print(data_path_config.keys())

    location = "office"  # "home"
    place = data_path_config[location]
    db_file_name = data_path_config['db_file_name']
    data_path_config['db_file'] = create_abspath_file(place['db_path'], db_file_name)

    
