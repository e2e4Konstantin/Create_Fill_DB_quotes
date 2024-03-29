from pathlib import Path
import pandas as pd
from icecream import ic


from config import dbTolls
from sql_queries import sql_periods_queries, sql_origins, sql_items_queries
from files_features import output_message_exit


from files_features import create_abspath_file

from config import LocalData

from postgres_export.pg_sql_queries import pg_sql_queries
from postgres_export.pg_config_db import PostgresDB
from postgres_export.pg_config import AccessData, db_access


from tools.periods.get_periods import get_periods_range



def _query_to_csv(db: PostgresDB, csv_file: str, query: str, params=None) -> int:
    """Получает выборку из БД и сохраняет ее в csv файл."""
    # df = pd.read_sql_query(
    #     sql=query, params=params, con=db.connection)
    results = db.select_rows_dict_cursor(query, params)
    if results:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        df.to_csv(csv_file, mode="w", encoding="utf-8", header=True, index=False)
        return 0
    return 1


def export_table_periods(csv_file: str, pgr_access: AccessData):
    """Сохраняет таблицу Postgres Normative larix.period в CSV файл."""
    with PostgresDB(pgr_access) as db:
        query = pg_sql_queries["get_all_periods"]
        _query_to_csv(db, csv_file, query)
    print(f"Периоды из Postgres Normative выгружены в файл: {csv_file!r}")

    # query = pg_sql_queries["get_all_period_table"]
        # with open(csv_file, "w", encoding='utf-8') as file:
        #     db.cursor.copy_expert(query, file)





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
        export_catalog_to_csv_for_period_id(pgr_access, period['basic_id'], catalog_csv_file)
        period['quotes_catalog_csv_file'] = Path(catalog_csv_file).name

        # ==> Quotes Data ==>
        quote_csv_file = build_file_name(period, quote_path, file_tail='Quotes.csv')
        export_quotes_to_csv_for_period_id(pgr_access, period['basic_id'], quote_csv_file)
        period['quotes_data_csv_file'] = Path(quote_csv_file).name





def export_resource_catalog_to_csv_for_period_id(pgr_access: AccessData, period_id: int, csv_file: str) -> int:
    """ Записывает Дерево каталога из GROUP_WORK_PROCESS для id_периода в CSV файл. """
    if period_id > 0 and csv_file:
        with PostgresDB(pgr_access) as db:
            # larix.resource_classifier id
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





def export_resource_for_range_periods(location: LocalData, pgr_access: AccessData) -> int:
    """
    Из Postgres Normative выгружает данные по ресурсам и каталогу/дереву в CSV файлы.
    Список периодов получаем из атрибута класса LocalData, куда он загружен из SQLite БД периодов.
    Для каждого периода отдельно из списка выгружает 2 фала: каталог и ресурсы.
    """

    resources_path = location.resources_path

    for period in location.periods_data:
        print(period)
        # ==> Resources Catalog ==>
        catalog_csv_file = build_file_name(
            period, resources_path, file_tail='Resources_Catalog.csv'
        )
        export_resource_catalog_to_csv_for_period_id(
            pgr_access, period['basic_id'], catalog_csv_file
        )
        # добавить имя файла в конфиг
        period["resources_catalog_csv_file"] = Path(catalog_csv_file).name







if __name__ == "__main__":

    with LocalData("office") as local:
        supplement_min, supplement_max = 69, 72

        # 1. Выгрузить таблицу периодов
        export_table_periods(csv_file=local.periods_file, pgr_access = db_access["normative"])
        # 2. Получить данные нужных периодов и записать в конфиг
        local.periods_data = get_periods_range(
            db_file=local.db_file,
            origin_name="ТСН",
            period_item_type="supplement",
            supplement_min=supplement_min,
            supplement_max=supplement_max,
        )
        # 3. Выгрузить: каталог расценок и расценки для указанных периодов (номеров дополнений)
        export_quotes_for_range_periods(local, db_access["normative"])

        # Выгрузить: каталог ресурсов и ресурсы и расценки для указанных периодов
        export_resource_for_range_periods(local, db_access["normative"])


    # ==> Test
    # {'period_id': 167085727}
    # period_id = 149000015
    # query = pg_sql_queries["get_work_process_for_period_id"]
    # query_parameter = {'period_id': period_id}
    # csv_file = 'test.csv'
    # access = db_access['normative']
    # with PostgresDB(access) as db:
    #     _query_to_csv(db, csv_file, query, query_parameter)

