from typing import NamedTuple
from datetime import datetime


from config.const import CONFIG_FILE_NAME, DB_FILE_NAME
from config.tools_json_config import read_config_to_json, write_config_to_json
from files_features import create_abspath_file


first_data_path_config = {
    # 'last_date':                "",  # datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    'config_file_name':         CONFIG_FILE_NAME,
    'db_file_name':             DB_FILE_NAME,
    'periods_file_name':        "period_export_table.csv",
    'office': {
        'db_path':              r"C:\Users\kazak.ke\Documents\PythonProjects\DB",
        'config_path':          r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config",
        'periods_path':         r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Periods",
        'quote_catalog_path':   r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesCatalog",
        'quote_data_path':      r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesData",
        "resources_path":       r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Resources"
    },
    'home': {
        'db_path':              r"F:\Kazak\GoogleDrive\Python_projects\DB",
        'config_path':          r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config",
    },
    'periods_data': [],
}


class LocalData(NamedTuple):
    place_name: str
    config_file: str
    db_file: str
    periods_file: str
    quote_catalog_path: str
    quote_data_path: str
    resources_path: str
    periods_data: list = None


def get_data_location(location: str) -> LocalData:
    """ Формирует ссылки на данные в зависимости от места работы. """
    config_path = first_data_path_config[location]['config_path']
    config_file = create_abspath_file(config_path, CONFIG_FILE_NAME)
    config = read_config_to_json(config_file)

    location_paths  = LocalData(
        place_name = location,
        config_file = config_file,
        db_file = create_abspath_file(config[location]['db_path'], config['db_file_name']),
        periods_file = create_abspath_file(
            config[location]['periods_path'], config['periods_file_name']),
        quote_catalog_path = config[location]['quote_catalog_path'],
        quote_data_path = config[location]['quote_data_path'],
        resources_path=config[location]['resources_path'],
        periods_data=config["periods_data"]
    )
    return location_paths


def save_data_location(local_paths: LocalData) -> int:
    """ . """
    config = read_config_to_json(local_paths.config_file)
    # local_branch = local_paths._asdict()
    config['periods_data'] = local_paths.periods_data
    write_config_to_json(local_paths.config_file, config)
    return 0


if __name__ == "__main__":
    from icecream import ic

    # path = first_data_path_config['office']['config_path']
    # first_config_file = create_abspath_file(path, CONFIG_FILE_NAME)
    # ic(first_config_file)
    # write_config_to_json(first_config_file, first_data_path_config)

    # x = get_data_location("office")
    # x = x._replace(periods = [1, 2, 3, 4,])
    # save_data_location(x)

    # ic(x)
    # ic(x.__dir__())
    # ic(x._asdict())

