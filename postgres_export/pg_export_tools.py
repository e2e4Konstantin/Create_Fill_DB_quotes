import sqlite3
from pathlib import Path
import pandas as pd
from icecream import ic

from files_features import create_abspath_file

from config import LocalData
from sql_queries import sql_periods_queries

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.pg_config_db import PostgresDB
from postgres_export.pg_config import AccessData, db_access
from config import dbTolls

from tools import get_period_range


def _query_to_csv(db: PostgresDB, csv_file: str, query: str, params=None) -> int:
    """ Получает выборку из БД и сохраняет ее в csv файл. """
    # Check if db is not None
    if db is None:
        raise ValueError("db shouldn't be None")
    # Check if csv_file is not None
    if csv_file is None:
        raise ValueError("csv_file shouldn't be None")
    # Check if query is not None
    if query is None:
        raise ValueError("query shouldn't be None")
    results = None
    # df = pd.read_sql_query(
    #     sql=query, params=params, con=db.connection)
    try:
        results = db.select_rows_dict_cursor(query, params)
    except Exception as ex:
        raise RuntimeError("An exception occurred when getting data from the DB") from ex

    if results:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        df.to_csv(csv_file, mode="w", encoding="utf-8", header=True, index=False)
        return 0
    return 1



def export_table_periods_to_csv(csv_file: str, pgr_access: AccessData):
    """Сохраняет таблицу Postgres Normative larix.period в CSV файл."""
    with PostgresDB(pgr_access) as db:
        query = pg_sql_queries["get_all_periods"]
        _query_to_csv(db, csv_file, query)
    # message = f"Периоды из Postgres Normative выгружены в файл: {csv_file!r}"
    # ic(message)

    # query = pg_sql_queries["get_all_period_table"]
    # with open(csv_file, "w", encoding='utf-8') as file:
    #     db.cursor.copy_expert(query, file)


def get_period_id_by_name(pgr_access: AccessData, period_title: str) -> int | None:
    """получить id периода по названию"""
    if period_title:
        with PostgresDB(pgr_access) as db:
            query = pg_sql_queries["get_period_id"]
            query_parameter = {"period_title": period_title}
            result = db.select_rows_dict_cursor(query, query_parameter)
            if result:
                return result[0]["period_id"]
    return None


def export_catalog_to_csv_for_period_id(
    pgr_access: AccessData, period_id: int, csv_file: str
) -> int:
    """Записывает Дерево каталога из GROUP_WORK_PROCESS для id_периода в CSV файл."""
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            # ic(db.connection.dsn)
            query = pg_sql_queries["get_group_work_process_for_period_id"]
            query_parameter = {"period_id": period_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1


def export_quotes_to_csv_for_period_id(
    pgr_access: AccessData, period_id: int, csv_file: str
) -> int:
    """Записывает Расценки из WORK_PROCESS для id_периода в CSV файл."""
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            query = pg_sql_queries["get_work_process_for_period_id"]
            query_parameter = {"period_id": period_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1


# [{'basic_id': 167085727, 'id': 72, 'title': 'Дополнение 72'},
#  {'basic_id': 166998701, 'id': 163, 'title': 'Индекс 209'},...
def build_file_name(
    period_info: dict[str:any], path: str, file_tail: str
) -> str | None:
    """Сделать имя файла из информации по периоду и хвосту файла."""
    title = period_info["title"].replace(" ", "_")
    head = file_tail.split(".")
    name = "_".join([head[0], title, "id", str(period_info["basic_id"])])
    return create_abspath_file(path, f"{name}.{head[1]}")


def export_quotes_for_range_periods(location: LocalData, pgr_access: AccessData) -> int:
    """
    Из Postgres Normative выгружает данные по расценкам и каталогу/дереву в CSV файлы.
    Список периодов получаем из атрибута класса LocalData, куда он загружен из SQLite БД периодов.
    Для каждого периода отдельно из списка выгружает 2 фала: каталог и расценки.
    """

    quote_path = location.quote_data_path
    quote_catalog_path = location.quote_catalog_path

    for period in location.periods_data:
        # ==> Quotes Catalog ==>
        catalog_csv_file = build_file_name(
            period, quote_catalog_path, file_tail="Catalog_Quotes.csv"
        )
        export_catalog_to_csv_for_period_id(
            pgr_access, period["basic_id"], catalog_csv_file
        )
        period["quotes_catalog_csv_file"] = Path(catalog_csv_file).name

        # ==> Quotes Data ==>
        quote_csv_file = build_file_name(period, quote_path, file_tail="Quotes.csv")
        export_quotes_to_csv_for_period_id(
            pgr_access, period["basic_id"], quote_csv_file
        )
        period["quotes_data_csv_file"] = Path(quote_csv_file).name


def export_resource_catalog_to_csv_for_period_id(
    pgr_access: AccessData, period_id: int, csv_file: str
) -> int:
    """Записывает Дерево каталога из GROUP_WORK_PROCESS для id_периода в CSV файл."""
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            # larix.resource_classifier id
            result = db.select_rows_dict_cursor(
                pg_sql_queries["get_origin_id"], {"origin_title": "ТСН"}
            )
            if result:
                origin_id = result[0][0]
            else:
                return 1
            #  получить записи дерева ресурсов для id периода и id resource_classifier
            #  %(period_id)s  %(origin_id)s
            query = pg_sql_queries["get_group_resource_for_period_id_origin_id"]

            query_parameter = {"period_id": period_id, "origin_id": origin_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1


def export_resources_to_csv_for_period_id(pgr_access: AccessData, period_id: int, csv_file: str
) -> int:
    """Записывает Ресурсы из ????_PROCESS для id_периода в CSV файл."""
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            query = pg_sql_queries["get_resources_1_2_for_period_id"]
            query_parameter = {"period_id": period_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1


def export_resource_for_range_periods(
    location: LocalData, pgr_access: AccessData
) -> int:
    """
    Из Postgres Normative выгружает данные по ресурсам и каталогу/дереву в CSV файлы.
    Список периодов получаем из атрибута класса LocalData, куда он загружен из SQLite БД периодов.
    Для каждого периода отдельно из списка выгружает 2 фала: каталог и ресурсы.
    """
    resources_path = location.resources_path
    for period in location.periods_data:
        # ic(period)
        # ==> Resources Catalog ==>
        catalog_csv_file = build_file_name(
            period, resources_path, file_tail="Resources_Catalog.csv"
        )
        export_resource_catalog_to_csv_for_period_id(
            pgr_access, period["basic_id"], catalog_csv_file
        )
        # добавить имя файла в конфиг
        period["resources_catalog_csv_file"] = Path(catalog_csv_file).name

        # ==> Resources Data 1 and 2 ==>
        resources_csv_file = build_file_name(
            period, resources_path, file_tail="Resources.csv"
        )
        export_resources_to_csv_for_period_id(
            pgr_access, period["basic_id"], resources_csv_file
        )
        period["resources_data_csv_file"] = Path(resources_csv_file).name


def export_storage_cost_to_csv_for_period_range(
        pgr_access: AccessData, csv_file: str, period_id_range: tuple[int, ...]=None
) -> int:
    """
    Записывает ЗСР для списка id периодов  в CSV файл.
    """
    with PostgresDB(pgr_access) as db:
        # larix.storage_cost
        query = pg_sql_queries["get_storage_costs_for_period_id_range"]
        query_parameter = {"period_id_range": period_id_range}
        result = _query_to_csv(db, csv_file, query, query_parameter)
        return result
    return 1


def export_transport_cost_to_csv_for_period_range(
        pgr_access: AccessData, csv_file: str, period_id_range: tuple[int, ...]=None
) -> int:
    with PostgresDB(pgr_access) as db:
        # larix.transport_cost
        query = pg_sql_queries["get_transport_costs_for_period_id_range"]
        query_parameter = {"period_id_range": period_id_range}
        result = _query_to_csv(db, csv_file, query, query_parameter)
        return result
    return 1


def export_material_properties_to_csv_for_period_range(
    pgr_access: AccessData, csv_file: str, period_id_range: tuple[int, ...] = None
) -> int:
    """
    Выгрузка свойств материалов Глава 1,
    кроме 0-й главы (транспортные расходы хранятся в tblTransportCost)
    """
    with PostgresDB(pgr_access) as db:
        # larix.transport_cost
        query = pg_sql_queries["get_materials_properties_for_period_id_range"]
        query_parameter = {"period_id_range": period_id_range}
        result = _query_to_csv(db, csv_file, query, query_parameter)
        return result
    return 1





if __name__ == "__main__":
    """
    Экспортируем таблицу из (Postgres Normative larix.period) периодов в csv файл.
    В main_src_loading.py загружаем периоды. Задаем диапазон периодов.
    Записываем данные в конфиг/словарь/файл.

    Экспортируем  Расценки и Ресурсы для созданного диапазона периодов в свои csv файлы,
    Сохраняем имена файлов и маршруты в конфиг (при закрытии контекстного менеджера ).
    """
    with LocalData("office") as local:
        # Выгрузить таблицу периодов / это делается в db_data_prepare(location)
        # при создании таблиц БД

        # export_table_periods_to_csv(
        #     csv_file=local.periods_file, pgr_access=db_access["normative"]
        # )

        # # 3. Выгрузить: каталог расценок и расценки для периодов,
        # #  данные которых записаны в конфиг. файл
        # export_quotes_for_range_periods(local, db_access["normative"])

        # # 4. Выгрузить: каталог ресурсов и ресурсы для конфиг периодов
        # export_resource_for_range_periods(local, db_access["normative"])

        # 5. Создать диапазон Normative_id индексных периодов
        normative_index_periods = tuple([
            indexes[0]
            for supplement in local.periods_data if supplement["indexes"] is not None
            for indexes in supplement["indexes"]
        ])
        print(normative_index_periods)
        # # 6. Выгрузить %ЗСР
        # export_storage_cost_to_csv_for_period_range(
        #     db_access["normative"], local.storage_costs_file, normative_index_periods
        # )
        # # 6. Выгрузить Транспортные расходы
        # export_transport_cost_to_csv_for_period_range(
        #     db_access["normative"], local.transport_costs_file, normative_index_periods
        # )
        # 7. Выгрузить Свойства Материалов Глава 1,
        export_material_properties_to_csv_for_period_range(
            db_access["normative"],
            local.material_properties_file,
            normative_index_periods,
        )

