from config import LocalData
from postgres_export.pg_export_tools import (
    export_quotes_for_range_periods,
    export_resource_for_range_periods,
    export_storage_cost_to_csv_for_period_range,
    export_transport_cost_to_csv_for_period_range,
    export_material_properties_to_csv_for_period_range,
)
from postgres_export.pg_config import db_access

if __name__ == "__main__":
    """
    Экспортируем таблицу из (Postgres Normative larix.period) периодов в csv файл.
    В main_src_loading.py загружаем периоды. Задаем диапазон периодов.
    Записываем данные в конфиг/словарь/файл.

    Экспортируем  Расценки и Ресурсы для созданного диапазона периодов в свои csv файлы,
    Сохраняем имена файлов и маршруты в конфиг (при закрытии контекстного менеджера ).
    """
    with LocalData("office") as local:
        # 1. Выгрузить таблицу периодов / это делается в
        # Create_Fill_DB_quotes\main_src_loading.py
        # db_data_prepare(location) при создании таблиц БД

        # 3. Выгрузить: каталог расценок и расценки для периодов,
        #  данные которых записаны в конфиг.файл config_quotes_parsing.json
        export_quotes_for_range_periods(local, db_access["normative"])

        # 4. Выгрузить: каталог ресурсов и ресурсы для конфиг периодов
        export_resource_for_range_periods(local, db_access["normative"])

        # 5. Создать диапазон Normative_id индексных периодов
        normative_index_periods = tuple(
            [
                indexes[0]
                for supplement in local.periods_data
                if supplement["indexes"] is not None
                for indexes in supplement["indexes"]
            ]
        )
        print(normative_index_periods)
        # 6. Выгрузить %ЗСР
        export_storage_cost_to_csv_for_period_range(
            db_access["normative"], local.storage_costs_file, normative_index_periods
        )
        # 6. Выгрузить Транспортные расходы
        export_transport_cost_to_csv_for_period_range(
            db_access["normative"], local.transport_costs_file, normative_index_periods
        )
        # 7. Выгрузить Свойства Материалов Глава 1,
        export_material_properties_to_csv_for_period_range(
            db_access["normative"],
            local.material_properties_file,
            normative_index_periods,
        )