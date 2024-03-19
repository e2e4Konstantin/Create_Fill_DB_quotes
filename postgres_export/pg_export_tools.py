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


def _query_to_csv(db: PostgresDB, csv_file_name: str, query: str, params=None) -> int:
    """ Получает выборку из БД и сохраняет ее в csv файл. """
    # df = pd.read_sql_query(
    #     sql=query, params=params, con=db.connection)
    results = db.select_rows_dict_cursor(query, params)
    if results:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        df.to_csv(csv_file_name, mode='w', encoding='utf-8', header=True, index=False)
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




def export_catalog_for_range_periods(
        my_location: str,
        pgr_access: AccessData, 
        supplement_min: int, supplement_max: int
    ) -> int:
    """ Выгружает данные каталога в CSV файлы для каждого периода отдельно. """
    data_paths: LocalData = get_data_location(my_location)
    catalog_path = data_paths.quote_catalog_path

    exported_periods = get_min_max_periods(
        data_paths.db_file, supplement_min, supplement_max)

    for period in exported_periods:
        csv_file = build_file_name(period, catalog_path, file_tail='Catalog.csv')
        r = export_catalog_to_csv_for_period_id(pgr_access, period['basic_id'], csv_file)
        # ic(r)
        period['catalog_csv_file'] = Path(csv_file).name
    data_paths = data_paths._replace(periods_data=exported_periods)
    save_data_location(data_paths)
    # ic(exported_periods)


def export_quotes_for_range_periods(
    my_location: str,
    pgr_access: AccessData,
    supplement_min: int, supplement_max: int
) -> int:
    """ Выгружает данные по расценкам в CSV файлы для каждого периода отдельно. """
    data_paths: LocalData = get_data_location(my_location)
    quote_path = data_paths.quote_data_path
    
    periods_band = get_supplement_periods(
        data_paths.db_file, supplement_min, supplement_max)

    for period in periods_band:
        quote_csv_file = build_file_name(
            period, quote_path, file_tail='Quotes.csv')

        export_quotes_to_csv_for_period_id(
            pgr_access, period['basic_id'], quote_csv_file)

        period['quotes_csv_file'] = Path(quote_csv_file).name
    data_paths = data_paths._replace(periods_data=periods_band)
    save_data_location(data_paths)



if __name__ == "__main__":
    location = "office"
    # export_catalog_for_range_periods(location, db_access['normative'],
    #                                 supplement_min=67, supplement_max=72)

    export_quotes_for_range_periods(
        location, db_access['normative'],
        supplement_min=67, supplement_max=72)
    
    # {'period_id': 167085727}
    # period_id = 149000015
    # query = pg_sql_queries["get_work_process_for_period_id"]
    # query_parameter = {'period_id': period_id}
    # csv_file = 'test.csv'
    # access = db_access['normative']
    # with PostgresDB(access) as db:
    #     _query_to_csv(db, csv_file, query, query_parameter)

