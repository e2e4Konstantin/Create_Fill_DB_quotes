from icecream import ic

from tools import (
    db_create_tables_and_fill_directory,
    parsing_quotes,
    parsing_quotes_for_supplement,
    parsing_resources,
    parsing_resources_for_supplement,
    parsing_material_properties,
    delete_raw_table,
)
from tools import (
    get_period_range,
    parsing_raw_periods,
    get_periods_range,
    parsing_storage_cost,
    parsing_transport_cost,
    get_indexes_for_supplement,
    update_product_default_value_record,
)

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
        # создать таблицы, индексы .... заполнить справочники
        db_create_tables_and_fill_directory(db_file)

        # выгрузить таблицу периодов из Postgres Normative
        export_table_periods_to_csv(
            csv_file=local.periods_file, pgr_access=db_access["normative"]
        )
        parsing_raw_periods(local)

        # создать диапазон дополнений. (начиная с 69 дополнения изменилась модель расчета)
        supplement_min, supplement_max = 69, 71
        local.periods_data = get_periods_range(
            db_file=local.db_file, origin_name="ТСН", period_item_type="supplement",
            supplement_min=supplement_min, supplement_max=supplement_max,
        )
        # для каждого дополнения 3 индексных периода
        for supplement in local.periods_data:
            indexes = get_indexes_for_supplement(local.db_file, supplement["supplement"])
            supplement["indexes"] = indexes
            ic(indexes)



if __name__ == '__main__':

    location = "office" # office  # home
    # db_data_prepare(location)
    # !!! восстанови из копии конфиг файл

    local = LocalData(location)
    ic()
    for supplement in local.periods_data:    #[:1]:
        parsing_quotes_for_supplement(local, supplement)
        parsing_resources_for_supplement(local, supplement)

        ic(supplement["indexes"])
        for index_period in supplement["indexes"]:
            update_product_default_value_record(
                local.db_file, catalog="ТСН", period_id=index_period[3]
            )
            parsing_storage_cost(local, index_period)
            parsing_transport_cost(local, index_period)
            parsing_material_properties(local, index_period)
    delete_raw_table(local.db_file)
