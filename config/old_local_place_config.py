from typing import NamedTuple
from datetime import datetime
from typing import Literal


from config.const import CONFIG_FILE_NAME, DB_FILE_NAME
from config.tools_json_config import read_config_to_json, write_config_to_json
from files_features import create_abspath_file


src_config = {
    "config_file_name": CONFIG_FILE_NAME,
    "db_file_name": DB_FILE_NAME,
    "periods_file_name": "period_export_table.csv",
    "office": {
        "db_path": r"C:\Users\kazak.ke\Documents\PythonProjects\DB",
        "config_path": r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config",
        "periods_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Periods",
        "quote_catalog_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesCatalog",
        "quote_data_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesData",
        "resources_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Resources",
    },
    "periods_data": [],
}


class LocalData:
    SRC = {
        "config_file_name": CONFIG_FILE_NAME,
        "db_file_name": DB_FILE_NAME,
        "periods_file_name": "period_export_table.csv",
        "office": {
            "db_path": r"C:\Users\kazak.ke\Documents\PythonProjects\DB",
            "config_path": r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config",
            "periods_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Periods",
            "quote_catalog_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesCatalog",
            "quote_data_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesData",
            "resources_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Resources",
        },
        "periods_data": [],
    }

    def __init__(self, location: str):
        self.place_name: Literal["office", "home"] = location
        self.config_file: str = create_abspath_file(
            LocalData.SRC[location]["config_path"], LocalData.SRC["config_file_name"]
        )
        self.db_file: str = create_abspath_file(
            src_config[location]["db_path"], src_config["db_file_name"]
        )
        self.periods_file: str = create_abspath_file(
            src_config[location]["periods_path"], src_config["periods_file_name"]
        )
        self.quote_catalog_path: str = src_config[location]["quote_catalog_path"]
        self.quote_data_path: str = src_config[location]["quote_data_path"]
        self.resources_path: str = src_config[location]["resources_path"]
        self.periods_data: list = None

    def save_config(self):
        config = {}
        config["periods_data"] = self.periods_data
        config['last_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        write_config_to_json(self.config_file, config)






if __name__ == "__main__":
    from icecream import ic
    ic()
    # path = first_data_path_config['office']['config_path']
    # first_config_file = create_abspath_file(path, CONFIG_FILE_NAME)
    # ic(first_config_file)
    # write_config_to_json(first_config_file, first_data_path_config)

    x = get_data_location("office")
    x.save_config()

    # ic(x)
    # ic(x.__dir__())
    # ic(x._asdict())

