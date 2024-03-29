from pathlib import Path
from postgres_export.pg_config import AccessData
from icecream import ic
import pandas as pd

from files_features import create_abspath_file

from config import dbTolls, LocalData, get_data_location, save_data_location

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.pg_config_db import PostgresDB
from postgres_export.pg_config import AccessData, db_access

from tools.periods.get_periods import get_supplement_periods


def export_table_periods(location: str, pgr_access: AccessData):
    """ Сохраняет таблицу larix.period в CSV файл. """

    data_paths: LocalData = get_data_location(location)
    csv_file = data_paths.periods_file
    with PostgresDB(pgr_access) as db:
        query = pg_sql_queries["get_all_periods"]
        _query_to_csv(db, csv_file, query)

        # query = pg_sql_queries["get_all_period_table"]
        # with open(csv_file, "w", encoding='utf-8') as file:
        #     db.cursor.copy_expert(query, file)


def _query_to_csv(db: PostgresDB, csv_file_name: str, query: str, params=None) -> int:
    """ Получает выборку из БД и сохраняет ее в csv файл. """
    # df = pd.read_sql_query(
    #     sql=query, params=params, con=db.connection)
    results = db.select_rows_dict_cursor(query, params)
    if results:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        df.to_csv(csv_file_name, mode='w', encoding='utf-8', header=True, index=False)
        print(f"df выгружен в файл {csv_file_name!r}")
        return 0
    return 1


def get_period_id_by_name(pgr_access: AccessData, period_title: str) -> int | None:
    """ получить id периода по названию """
    if period_title:
        with PostgresDB(pgr_access) as db:
            query = pg_sql_queries["get_period_id"]
            query_parameter = {'period_title': period_title}
            result = db.select_rows_dict_cursor(query, query_parameter)
            if result:
                return result[0]['period_id']
    return None



def export_catalog_to_csv_for_period_id(pgr_access: AccessData, period_id: int, csv_file: str) -> int:
    """ Записывает Дерево каталога из GROUP_WORK_PROCESS для id_периода в CSV файл. """
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            # ic(db.connection.dsn)
            query = pg_sql_queries["get_group_work_process_for_period_id"]
            query_parameter = {'period_id': period_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1


def export_quotes_to_csv_for_period_id(pgr_access: AccessData, period_id: int, csv_file: str) -> int:
    """ Записывает Расценки из WORK_PROCESS для id_периода в CSV файл. """
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            query = pg_sql_queries["get_work_process_for_period_id"]
            query_parameter = {'period_id': period_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1



# [{'basic_id': 167085727, 'id': 72, 'title': 'Дополнение 72'},
#  {'basic_id': 166998701, 'id': 163, 'title': 'Индекс 209'},...
def build_file_name(period_info: dict[str: any], path: str, file_tail: str) -> str | None:
    """ Сделать имя файла из информации по периоду и хвосту файла. """
    title = period_info['title'].replace(' ', '_')
    head = file_tail.split('.')
    name = '_'.join([head[0], title, 'id', str(period_info['basic_id'])])
    return create_abspath_file(path, f"{name}.{head[1]}")



def export_quotes_for_range_periods(
    location: str, pgr_access: AccessData, supplement_min: int, supplement_max: int
    ) -> int:
    """ Выгружает данные по расценкам и дереву в CSV файлы для каждого периода отдельно.
        Отдельно файл Каталога и файл с Данными по расценкам.
        Получает данные по периодам Дополнений в заданном диапазоне.
        Для каждого периода выгружает файл с каталогом и файл с расценками.
    """
    data_paths: LocalData = get_data_location(location)

    db_file = data_paths.db_file
    quote_path = data_paths.quote_data_path
    quote_catalog_path = data_paths.quote_catalog_path

    periods_band = get_supplement_periods(db_file, supplement_min, supplement_max)

    for period in periods_band:
        # ==> Quotes Catalog ==>
        catalog_csv_file = build_file_name(period, quote_catalog_path, file_tail='Catalog.csv')
        export_catalog_to_csv_for_period_id(pgr_access, period['basic_id'], catalog_csv_file)
        period['quotes_catalog_csv_file'] = Path(catalog_csv_file).name
        # ==> Quotes Data ==>
        quote_csv_file = build_file_name(period, quote_path, file_tail='Quotes.csv')
        export_quotes_to_csv_for_period_id(pgr_access, period['basic_id'], quote_csv_file)
        period['quotes_data_csv_file'] = Path(quote_csv_file).name

    data_paths = data_paths._replace(periods_data=periods_band)
    save_data_location(data_paths)




def export_resource_catalog_to_csv_for_period_id(pgr_access: AccessData, period_id: int, csv_file: str) -> int:
    """ Записывает Дерево каталога из GROUP_WORK_PROCESS для id_периода в CSV файл. """
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            result = db.select_rows_dict_cursor(pg_sql_queries["get_origin_id"], {'origin_title': 'ТСН'})
            if result:
                origin_id = result[0][0]
            else:
                return 1
            #  получить записи дерева ресурсов для id периода и id resource_classifier
            #  %(period_id)s  %(origin_id)s
            query = pg_sql_queries["get_group_resource_for_period_id_origin_id"]

            query_parameter = {'period_id': period_id, 'origin_id': origin_id}
            result = _query_to_csv(db, csv_file, query, query_parameter)
            return result
    return 1





def export_resource_for_range_periods(
    location: str, pgr_access: AccessData, supplement_min: int, supplement_max: int
) -> int:
    """ Выгружает данные по ресурсам и дереву ресурсов в CSV файлы для каждого периода отдельно.
        Отдельно файл Каталога/Дерева и файл с Данными по Ресурсам.
        Получает данные по периодам Дополнений в заданном диапазоне.
        Для каждого периода выгружает файл с каталогом и файл с расценками.
    """
    data_paths: LocalData = get_data_location(location)

    db_file = data_paths.db_file
    resources_path = data_paths.resources_path

    periods_band = get_supplement_periods(
        db_file, supplement_min, supplement_max)

    for period in periods_band:
        print(period)
        # ==> Resources Catalog ==>
        catalog_csv_file = build_file_name(
            period, resources_path, file_tail='Resources_Catalog.csv')
        export_resource_catalog_to_csv_for_period_id(
            pgr_access, period['basic_id'], catalog_csv_file)
        # добавить имя файла в конфиг
        period['quotes_catalog_csv_file'] = Path(catalog_csv_file).name



    data_paths = data_paths._replace(periods_data=periods_band)
    save_data_location(data_paths)




if __name__ == "__main__":
    location = "office"

    # Выгрузить таблицу периодов
    export_table_periods(location, db_access['normative'])

    # Выгрузить данные: каталог расценок и расценки для указанных периодов
    # export_quotes_for_range_periods(
    #     location, db_access['normative'],
    #     supplement_min=67, supplement_max=69)

    # Выгрузить данные: каталог расценок и расценки для указанных периодов
    # export_resource_for_range_periods(
    #     location, db_access['normative'],
    #     supplement_min=67, supplement_max=69)


    # ==> Test
    # {'period_id': 167085727}
    # period_id = 149000015
    # query = pg_sql_queries["get_work_process_for_period_id"]
    # query_parameter = {'period_id': period_id}
    # csv_file = 'test.csv'
    # access = db_access['normative']
    # with PostgresDB(access) as db:
    #     _query_to_csv(db, csv_file, query, query_parameter)

