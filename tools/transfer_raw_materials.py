import sqlite3
from icecream import ic

from config import dbTolls, teams
from sql_queries import sql_raw_queries, sql_products_queries, sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit
from tools.code_tolls import clear_code, text_cleaning, get_integer_value

from tools.shared_features import (
    get_catalog_id_by_period_code, update_product, insert_product,
    get_product_row_by_code, delete_last_period_product_row
)


def _get_parent_catalog_id(db: dbTolls, raw_parent_id: int, period: int) -> int | None:
    """ По raw_parent_id родителя из сырой таблицы tblRawData из колонки 'PARENT'.
        Находит его в tblRawData и берет его шифр.
        Ищет в Каталоге запись с таким шифром и периодом возвращает его id
    """
    select = db.go_select(sql_raw_queries["select_rwd_material_cmt_id"], (raw_parent_id,))
    parent_code = clear_code(select[0]['CMT']) if select else None
    if parent_code:
        return get_catalog_id_by_period_code(db=db, period=period, code=parent_code)
    output_message_exit(f"в таблице tblRawData",f" не найден родитель с 'ID' {raw_parent_id!r}")
    return None


def _make_data_from_raw_material(db: dbTolls, raw_material: sqlite3.Row, material_type_id: int) -> tuple | None:
    """ Получает строку из таблицы tblRawData с импортированным материалом и id типа записи для материала.
        Ищет в Каталоге родительскую запись по шифру и периоду.
        Выбирает и готовит нужные данные.
        Возвращает кортеж с данными для вставки в таблицу Расценок.
    """
    raw_period = get_integer_value(raw_material["PERIOD"])
    # указатель на родительскую запись каталога
    holder_id = _get_parent_catalog_id(db, raw_parent_id=raw_material['PARENT'], period=raw_period)
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
        output_message_exit(f"в RAW таблице с Машинами не найдено ни одной записи:",
                            f"tblRawData пустая.")
        return None
    return raw_materials


def transfer_raw_data_to_materials(db_filename: str):
    """ Записывает МАТЕРИАЛЫ из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется материал с таким же шифром, если такой есть то он обновляется,
        если не найден то вставляется новый материал.
    """
    with dbTolls(db_filename) as db:
        query_index = """CREATE INDEX IF NOT EXISTS idxTmpMaterials ON tblRawData (ID, PARENT, CMT);"""
        query_drop_index = """DROP INDEX IF EXISTS idxTmpMaterial;"""
        db.go_execute(query_index)


        # получить все материалы из raw таблицы
        raw_data = _get_raw_data_materials(db)
        if not raw_data:
            return None
        # получить id типа для материала
        material_team, material_name = ("units", "material")
        material_type_id = db.get_row_id(
            sql_items_queries["select_item_id_team_name"], (material_team, material_name)
        )
        if not material_type_id:
            output_message_exit(f"в Справочнике 'units':", f"не найдена запись 'material'")
            return None
        inserted_success, updated_success = [], []
        for row_count, row in enumerate(raw_data):
            raw_code = clear_code(row['CMT'])
            raw_period = get_integer_value(row['PERIOD'])
            # Найти запись с шифром raw_cod в таблице tblProducts
            material = get_product_row_by_code(db=db, product_code=raw_code)
            if material:
                if raw_period >= material['period'] and material_type_id == material['FK_tblProducts_tblItems']:
                    data = _make_data_from_raw_material(db, row, material_type_id) + (material['ID_tblProduct'],)
                    count_updated = update_product(db, data)
                    if count_updated:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка Обновления Расценки: {raw_code!r} или item_type не совпадает {material_type_id}",
                        f"период Расценки {material['period']} старше загружаемого {raw_period}")
            else:
                data = _make_data_from_raw_material(db, row, material_type_id)
                inserted_id = insert_product(db, data)
                if inserted_id:
                    inserted_success.append((id, raw_code))
        row_count += 1
        alog = f"Всего записей в raw таблице: {row_count}."
        ilog = f"Добавлено {len(inserted_success)} расценок."
        ulog = f"Обновлено {len(updated_success)} расценок."
        none_log = f"Непонятных записей: {row_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)
        db.go_execute(query_drop_index)


    # удалить из Материалов записи период которых меньше чем максимальный период
    delete_last_period_product_row(db_filename, team=material_team, name=material_name)



if __name__ == '__main__':
    import os

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")
    ic(db_name)

    transfer_raw_data_to_materials(db_name)
