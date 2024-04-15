
from datetime import datetime
from typing import Literal


from config.const import CONFIG_FILE_NAME, DB_FILE_NAME
from config.tools_json_config import read_config_to_json, write_config_to_json
from files_features import create_abspath_file


class LocalData:
    """ класс для чтения, хранения, записи конфигурации фалов данных."""
    SRC = {
        "config_file_name": CONFIG_FILE_NAME,
        "db_file_name": DB_FILE_NAME,
        "periods_file_name": "period_export_table.csv",
        "storage_cost_file_name": "storage_cost_export_table.csv",
        "transport_cost_file_name": "transport_cost_export_table.csv",
        "machine_properties_file_name": "machine_properties_export.csv",
        "office": {
            "db_path": r"C:\Users\kazak.ke\Documents\PythonProjects\DB",
            "config_path": r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config",
            "periods_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Periods",
            "quote_catalog_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesCatalog",
            "quote_data_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Quotes\QuotesData",
            "resources_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Resources",
            "storage_costs_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\StorageCost",
            "transport_costs_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\TransportCosts",
            "machine_properties_path": r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\MachineProperties",
        },
        "home": {
            "db_path": r"..\DB",
            "config_path": r"..\Create_Fill_DB_quotes\config",
            "periods_path": r"\..\Periods",
            "quote_catalog_path": r"\..\Quotes\QuotesCatalog",
            "quote_data_path": r"\..\Quotes\QuotesData",
            "resources_path": r"\..\Resources",
            "storage_costs_path": r"..\StorageCosts",
        },
        # список периодов
        "periods_data": [],
        "index_period_range": [],
    }

    def __init__(self, location: str):
        self.place_name: Literal["office", "home"] = location
        self.config_file: str = create_abspath_file(
            LocalData.SRC[location]["config_path"], LocalData.SRC["config_file_name"]
        )
        self.db_file: str = create_abspath_file(
            LocalData.SRC[location]["db_path"], LocalData.SRC["db_file_name"]
        )
        self.periods_file: str = create_abspath_file(
            LocalData.SRC[location]["periods_path"], LocalData.SRC["periods_file_name"]
        )

        self.storage_costs_file: str = create_abspath_file(
            LocalData.SRC[location]["storage_costs_path"],
            LocalData.SRC["storage_cost_file_name"],
        )
        self.transport_costs_file: str = create_abspath_file(
            LocalData.SRC[location]["transport_costs_path"],
            LocalData.SRC["transport_cost_file_name"],
        )

        self.machine_properties_file: str = create_abspath_file(
            LocalData.SRC[location]["machine_properties_path"],
            LocalData.SRC["machine_properties_file_name"],
        )

        self.quote_catalog_path: str = LocalData.SRC[location]["quote_catalog_path"]
        self.quote_data_path: str = LocalData.SRC[location]["quote_data_path"]
        self.resources_path: str = LocalData.SRC[location]["resources_path"]
        self.storage_costs_path: str = LocalData.SRC[location]["storage_costs_path"]
        self.transport_costs_path: str = LocalData.SRC[location]["transport_costs_path"]
        self.machine_properties_path: str = LocalData.SRC[location][
            "machine_properties_path"
        ]

        # читаем конфиг из файла
        config = read_config_to_json(self.config_file)
        if config:
            self.periods_data = config["periods_data"]
            self.index_period_range = config["index_period_range"]
        else:
            self.periods_data = []
            self.index_period_range = []

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.save_config()

    def __str__(self):
        period_out = "\n".join([f"Период: {period}" for period in self.periods_data])
        return f"Место: {self.place_name}\nDB: {self.config_file}\n{period_out}"

    def save_config(self):
        config = {}
        config["place"] = self.place_name
        config['last_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        config["config_file"] = self.config_file
        config["db_file"] = self.db_file
        config["periods_file"] = self.periods_file

        config["storage_costs_file"] = self.storage_costs_file
        config["transport_costs_file"] = self.transport_costs_file
        config["machine_properties_file"] = self.transport_costs_file
        #
        config["quote_catalog_path"] = self.quote_catalog_path
        config["quote_data_path"] = self.quote_data_path
        config["resources_path"] = self.resources_path

        config["storage_costs_path"] = self.storage_costs_path
        config["transport_costs_path"] = self.transport_costs_path
        config["machine_properties_path"] = self.transport_costs_path

        # сортируем периоды по убыванию номеров дополнений
        if self.periods_data:
            self.periods_data.sort(reverse=False, key=lambda x: x["supplement"])
        config["periods_data"] = self.periods_data
        if self.index_period_range:
            self.index_period_range.sort(reverse=False, key=lambda x: x[1])
        config["index_period_range"] = self.index_period_range

        write_config_to_json(self.config_file, config)






if __name__ == "__main__":
    from icecream import ic
    ic()

    # x = LocalData("office")
    # print(x)
    # x.save_config()

    with LocalData("office") as local:
        print(local)
        local.place_name = "****"