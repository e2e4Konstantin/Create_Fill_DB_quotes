import sqlite3
from icecream import ic

from config import dbTolls

from files_features import output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value
from sql_queries import sql_raw_queries

from tools.shared_features import (
    update_product, insert_product, get_parent_catalog_id,
    get_product_row_by_code, delete_last_period_product_row, get_directory_id, get_origin_id
)


def _make_data_from_raw_machine(db: dbTolls, raw_machine: sqlite3.Row, machine_item_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированной машиной и id типа записи для материала.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу tblProducts.
    """
    raw_period = get_integer_value(raw_machine["PERIOD"])
    holder_id = get_parent_catalog_id(db, raw_parent_id=raw_machine['PARENT'], period=raw_period)
    if holder_id is None:
        return None
    raw_code = clear_code(raw_machine['CMT'])
    description = text_cleaning(raw_machine['TITLE']).capitalize()
    measurer = text_cleaning(raw_machine['Ед.Изм.'])
    # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, period, code, description, measurer, full_code
    data = (holder_id, machine_item_id, raw_period, raw_code, description, measurer, None)
    return data


def _get_raw_data_machines(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все Машины из сырой таблицы без элементов каталога. """
    raw_machines = db.go_select(sql_raw_queries["select_rwd_machines"])
    if not raw_machines:
        output_message_exit(f"в RAW таблице с Машинами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_machines


def transfer_raw_data_to_machines(db_filename: str):
    """ Записывает МАШИНЫ из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется машина с таким же шифром, если такая есть то машина обновляется,
        если не найдена, то вставляется новая машина.
    """
    with dbTolls(db_filename) as db:
        db.go_execute(sql_raw_queries["create_index_raw_data"])
        raw_machines = _get_raw_data_machines(db)
        if raw_machines is None:
            return None
        team, name = "units", "machine"
        machines_item_id = get_directory_id(db, directory_team=team, item_name=name)
        inserted_success, updated_success = [], []
        origin_id = get_origin_id(db, origin_name='ТСН')
        for row in raw_machines:
            data_line = _make_data_from_raw_machine(db, row, machines_item_id) + (origin_id,)
            raw_code = clear_code(row['CMT'])
            raw_period = get_integer_value(row['PERIOD'])
            machines = get_product_row_by_code(db=db, product_code=raw_code)
            if machines:
                if raw_period >= machines['period'] and machines_item_id == machines['FK_tblProducts_tblItems']:
                    count_updated = update_product(db, data_line + (machines['ID_tblProduct'],))
                    if count_updated:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка обновления Машины: {raw_code!r} или item_type не совпадает {machines_item_id}",
                        f"период Расценки {machines['period']} старше загружаемого {raw_period}")
            else:
                inserted_id = insert_product(db, data_line)
                if inserted_id:
                    inserted_success.append((id, raw_code))
        row_count = len(raw_machines)
        alog = f"Всего записей в raw таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_success)} машин."
        ulog = f"Обновлено {len(updated_success)} машин."
        none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)
        db.go_execute(sql_raw_queries["delete_index_raw_data"])
    # удалить из Машин записи период которых меньше чем максимальный период
    delete_last_period_product_row(db_filename, team=team, name=name)


if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    ic(db_name)

    transfer_raw_data_to_machines(db_name)
