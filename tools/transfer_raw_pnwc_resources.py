import sqlite3
from icecream import ic

from config import dbTolls, PNWC_CATALOG

from files_features import output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value
from sql_queries import sql_raw_queries

from tools.shared_features import (
    update_product, insert_product, get_parent_catalog_id,
    get_product_by_code, delete_last_period_product_row, get_directory_id,
    get_catalog_id_by_origin_code, get_origin_id
)


def _make_data_from_raw_pnwc_resource(
        db: dbTolls, origin_id: int, raw_resource: sqlite3.Row, item_id: int
) -> tuple | None:
    """ Получает строку из таблицы tblRawData с ресурсами ПСМ и id типа записи для оборудования.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу tblProducts. """
    raw_code = clear_code(raw_resource['CODE'])
    parent_code: str = raw_code.split('-')[0]
    # получить указатель на родительскую запись каталога
    holder_id = get_catalog_id_by_origin_code(db, origin=origin_id, code=parent_code)
    if holder_id is None:
        return None
    raw_period = get_integer_value(raw_resource["PERIOD"])
    description = text_cleaning(raw_resource['TITLE']).capitalize()
    measurer = text_cleaning(raw_resource['MEASURE']).strip()
    # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, FK_tblProducts_tblOrigins,
    # period, code, description, measurer, full_code
    data = (holder_id, item_id, origin_id, raw_period, raw_code, description, measurer, None)
    return data


def _get_raw_data_pnwc_resources(db: dbTolls, pattern: str) -> list[sqlite3.Row] | None:
    """ Выбрать все ... из сырой таблицы без элементов каталога. """
    raw_pom_resources = db.go_select(sql_raw_queries["select_rwd_resources"], (pattern, ))
    if not raw_pom_resources:
        output_message_exit(f"в RAW таблице с Машинами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_pom_resources


def transfer_raw_data_to_pnwc_resources(db_filename: str, catalog_name: str):
    """ Записывает ресурсы из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется ресурс с таким же шифром, если такая есть то машина обновляется,
        если не найдена, то вставляется новая машина.
    """
    with dbTolls(db_filename) as db:
        resource_pattern = r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$"
        raw_resources = _get_raw_data_pnwc_resources(db, resource_pattern)
        if raw_resources is None:
            return None
        directory = "units"
        ic()
        catalog_id = get_origin_id(db, origin_name=catalog_name)

        for row in raw_resources:
            raw_code = clear_code(row['CODE'])
            match raw_code.split('.')[0]:
                case '71':
                    item_name = "material"
                case '72':
                    item_name = "machine"
                case '73':
                    item_name = "equipment"
                case _:
                    item_name = "material"
            # ic(catalog_name, item_name, raw_code)
            item_id = get_directory_id(db, directory_team=directory, item_name=item_name)
            inserted_success, updated_success = [], []
            data = _make_data_from_raw_pnwc_resource(db, catalog_id, row, item_id)
            raw_code, raw_period = data[4], data[3]
            product = get_product_by_code(db=db, origin_id=catalog_id, product_code=raw_code)
            if product:
                if raw_period >= product['period'] and item_id == product['FK_tblProducts_tblItems']:
                    count_updated = update_product(db, data + (product['ID_tblProduct'],))
                    if count_updated:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка обновления Ресурса: {raw_code!r} или item_type не совпадает {item_id=!r}",
                        f"период Продукта {product['period']} старше загружаемого {raw_period}")
            else:
                inserted_id = insert_product(db, data)
                if inserted_id:
                    inserted_success.append((id, raw_code))
        row_count = len(raw_resources)
        alog = f"Всего записей в raw таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_success)} оборудования."
        ulog = f"Обновлено {len(updated_success)} оборудования."
        none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)
        db.go_execute(sql_raw_queries["delete_index_raw_data"])
        # удалить item записи период которых меньше чем максимальный период
        delete_last_period_product_row(db, origin_id=catalog_id, team=directory, name=item_name)


if __name__ == '__main__':
    import os
    from read_csv import read_csv_to_raw_table

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
    pom_resource = os.path.join(data_path, "Данные_НЦКР_Временный_каталог_НЦКР_2023_4_кв.csv")
    ic(db_name)

    read_csv_to_raw_table(
        db_name, pom_resource, set_period=0,
        new_column_name=['N', 'NPP', 'Шифр новый действующий', 'Уточненное наименование по данным мониторинга']
    )
    transfer_raw_data_to_pnwc_resources(db_name, catalog_name=PNWC_CATALOG)
