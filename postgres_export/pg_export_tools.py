
from postgres_export.pg_config import AccessData
from icecream import ic
import pandas as pd

from files_features import create_abspath_file
from data_path import set_data_location
from config import dbTolls

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.pg_config_db import PostgresDB
from postgres_export.pg_config import AccessData, db_access

from tools.periods.get_min_max_periods import get_min_max_periods


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
    """ Записывает Дерево каталога для указанного периода id в scv файл. """
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            # ic(db.connection.dsn)
            query = pg_sql_queries["get_group_work_process_for_period_id"]
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
        supplement_min: int, supplement_max: int,
        index_min: int, index_max: int
    ) -> int:
    """ """
    local = set_data_location(my_location)
    catalog_path = r'C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct\Catalog'

    exported_periods = get_min_max_periods(
        local.db_file, supplement_min, supplement_max, index_min, index_max)

    for period in exported_periods:
        csv_file = build_file_name(period, catalog_path, file_tail='Catalog.csv')
        r = export_catalog_to_csv_for_period_id(pgr_access, period['basic_id'], csv_file)
        ic(r)
        period['catalog_csv_file'] = csv_file
    save_json_config(exported_periods)
    ic(choice_periods)





if __name__ == "__main__":
    location = "office"
    export_catalog_for_range_periods(
        location, db_access['normative'], 
        supplement_min=67, supplement_max=72, index_min=195, index_max=209)

    # path_output = r'C:\Users\kazak.ke\Documents\АИС_Выгрузка\Postgres_Direct'
    # period = {'basic_id': 166998701, 'id': 163, 'title': 'Индекс 209'}
    # csv_file = build_file_name(period, path=path_output, file_tail='Catalog.csv')
    # ic(csv_file)
    # period['catalog_csv_file'] = csv_file
    # ic(period)

    # pid = get_period_id_by_name(
    #     db_access['normative'], period_title='Дополнение 67')
    # ic(pid)

    # r = export_catalog_to_csv_for_period_id(db_access['normative'], period['basic_id'], csv_file)
    # ic(r)



