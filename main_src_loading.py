from icecream import ic

from tools import (
    db_create_tables_and_fill_directory,
    parsing_quotes,
    parsing_resources,
    delete_raw_table,
)
from tools import parsing_raw_periods, get_periods_range, parsing_storage_cost
from config import LocalData
from postgres_export import (
    db_access,
    export_table_periods_to_csv,
    export_quotes_for_range_periods,
    export_resource_for_range_periods,
)


def db_data_prepare(location: str):
    """
    Создает чистую БД SQLite. Заполняет справочники.
    Записывает в каталог две головные записи для ТСН и НЦКР.
    Экспортирует периоды из Postgres.Normative larix.period в csv файл.
    Загружает периоды из scv файла в таблицу периодов tblPeriods.
    Формирует диапазон периодов для экспорта данных
    по расценкам и ресурсам из Postgres.Normative.
    Экспортируем расценки и ресурсы для диапазона периодов.
    Данные из LocalData записываются в файл конфигурации.
    """
    with LocalData(location) as local:
        db_file: str = local.db_file
        ic(location, db_file)

        db_create_tables_and_fill_directory(db_file)

        export_table_periods_to_csv(
            csv_file=local.periods_file, pgr_access=db_access["normative"]
        )

        parsing_raw_periods(local)

        supplement_min, supplement_max = 69, 72
        local.periods_data = get_periods_range(
            db_file=local.db_file, origin_name="ТСН", period_item_type="supplement",
            supplement_min=supplement_min, supplement_max=supplement_max,
        )
        export_quotes_for_range_periods(local, db_access["normative"])
        export_resource_for_range_periods(local, db_access["normative"])
    # Записать данные периодов в конфиг файл __exit__().


if __name__ == '__main__':

    location = "office" # office  # home

    db_data_prepare(location)

    local = LocalData(location)
    periods = [x["title"] for x in local.periods_data]
    ic(periods)

    parsing_quotes(local)
    parsing_resources(local)
    parsing_storage_cost(local)
    delete_raw_table(local.db_file)
