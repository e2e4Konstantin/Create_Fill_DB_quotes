import sqlite3
from icecream import ic

from config import dbTolls, teams
from sql_queries import sql_raw_queries, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value

from tools.shared_features import (
    get_parent_catalog_id, update_product, insert_product,
    get_product_row_by_code, delete_last_period_product_row, get_directory_id, get_origin_id
)


def _make_data_from_raw_material(db: dbTolls, raw_material: sqlite3.Row, material_type_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированным материалом и id типа записи для материала.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    raw_period = get_integer_value(raw_material["PERIOD"])
    # указатель на родительскую запись каталога
    holder_id = get_parent_catalog_id(db, raw_parent_id=raw_material['PARENT'], period=raw_period)
    if holder_id is None:
        return None
    raw_code = clear_code(raw_material['CMT'])
    description = text_cleaning(raw_material['TITLE']).capitalize()
    measurer = text_cleaning(raw_material['Ед.Изм.'])
    # FK_tblProducts_tblCatalogs, FK_tblProducts_tblItems, period, code, description, measurer, full_code
    data = (holder_id, material_type_id, raw_period, raw_code, description, measurer, None)
    return data


def _get_raw_data_materials(db: dbTolls) -> list[sqlite3.Row] | None:
    """ Выбрать все Материалы из сырой таблицы без элементов каталога. """
    raw_materials = db.go_select(sql_raw_queries["select_rwd_materials"])
    if not raw_materials:
        output_message_exit(f"в RAW таблице с Материалами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_materials


def transfer_raw_data_to_materials(db_filename: str):
    """ Записывает МАТЕРИАЛЫ из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется материал с таким же шифром, если такой есть то он обновляется,
        если не найден то вставляется новый материал.
    """
    with dbTolls(db_filename) as db:
        db.go_execute(sql_raw_queries["create_index_raw_data"])
        raw_materials = _get_raw_data_materials(db)
        if not raw_materials:
            return None
        # получить id типа для материала
        team, name = "units", "material"
        material_item_id = get_directory_id(db, directory_team=team, item_name=name)
        inserted_success, updated_success = [], []
        origin_id = get_origin_id(db, origin_name='ТСН')
        for row in raw_materials:
            data_line = _make_data_from_raw_material(db, row, material_item_id) + (origin_id,)
            raw_code = clear_code(row['CMT'])
            raw_period = get_integer_value(row['PERIOD'])
            material = get_product_row_by_code(db=db, product_code=raw_code)
            if material:
                if raw_period >= material['period'] and material_item_id == material['FK_tblProducts_tblItems']:
                    count_updated = update_product(db, data_line + (material['ID_tblProduct'],))
                    if count_updated:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка Обновления Материала: {raw_code!r} или item_type не совпадает {material_item_id}",
                        f"период Расценки {material['period']} старше загружаемого {raw_period}")
            else:
                inserted_id = insert_product(db, data_line)
                if inserted_id:
                    inserted_success.append((id, raw_code))
        row_count = len(raw_materials)
        alog = f"Всего записей в raw таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_success)} материалов."
        ulog = f"Обновлено {len(updated_success)} материалов."
        none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)
        db.go_execute(sql_raw_queries["delete_index_raw_data"])
    # удалить из Материалов записи период которых меньше чем максимальный период
    delete_last_period_product_row(db_filename, team=team, name=name)


if __name__ == '__main__':
    import os
    from tools import read_csv_to_raw_table, transfer_raw_data_to_catalog

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    data_path = r"F:\Kazak\GoogleDrive\NIAC\АИС_Выгрузка\csv"
    materials_data = os.path.join(data_path, "1_глава_67_доп.csv")
    ic(db_name)

    # read_csv_to_raw_table(db_name, materials_data, 67)
    # transfer_raw_data_to_catalog(db_name, directory='materials', main_code='1')
    transfer_raw_data_to_materials(db_name)

