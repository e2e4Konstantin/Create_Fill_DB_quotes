from config import LocalData, TON_ORIGIN
from icecream import ic

from tools.shared.excel_df_raw_table_transfer import load_csv_to_raw_table
from files_features import create_abspath_file

from tools.resources.transfer_raw_resources_1_2_to_catalog import transfer_raw_resource_to_catalog

from tools.resources.transfer_raw_resource_to_products import transfer_raw_resource_to_products




# заполнить каталог расценок и сами расценки
def parsing_resources(location: LocalData) -> int:
    """
    Заполняет Таблицы БД (tblCatalogs, tblProducts) каталог/дерево
    ресурсов и сами ресурсы. Читает данные из CSV файлов в tblRawData.
    Переносит данные из tblRawData в боевые таблицы.
    """
    print()
    ic("===>>> Загружаем Ресурсы (1 & 2).")
    resources_path: str = location.resources_path
    db_file: str = location.db_file
    for period in location.periods_data:  # [:1]:
        print()
        ic(period["title"])
        period_id = period["id"]

        # грузим каталог
        catalog_csv_file = create_abspath_file(
            resources_path, period["resources_catalog_csv_file"]
        )
        load_csv_to_raw_table(catalog_csv_file, db_file, delimiter=",")
        transfer_raw_resource_to_catalog(db_file, TON_ORIGIN, period_id)

        # грузим ресурсы
        data_csv_file = create_abspath_file(
            resources_path, period["resources_data_csv_file"]
        )
        load_csv_to_raw_table(data_csv_file, db_file, delimiter=",")
        transfer_raw_resource_to_products(db_file, TON_ORIGIN, period_id)
    return 0


if __name__ == "__main__":
    local = LocalData("office")  # office  # home
    parsing_resources(local)
