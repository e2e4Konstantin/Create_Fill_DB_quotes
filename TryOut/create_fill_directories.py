import os
from config import MAIN_RECORD_CODE, TON_ORIGIN, PNWC_ORIGIN
from tools.create.create_tables import create_tables_indexes
from tools.create.fill_directory import fill_directory_origins, fill_directory_catalog_items
from tools.create.insert_root_row_catalog import insert_root_record_to_catalog



def db_create_tables_and_fill_directory(db_file: str) -> int:
    """
    Удаляет файл БД если такой есть. Создает таблицы, индексы, триггеры.
    Заполняет справочник происхождения tblOrigins, справочник элементов каталога.
    Вставляет корневую запись для ТСН. Вставляет корневую запись в каталог для НЦКР.
    """
    if os.path.isfile(db_file):
        os.unlink(db_file)

    create_tables_indexes(db_file)
    fill_directory_origins(db_file)
    fill_directory_catalog_items(db_file)

    insert_root_record_to_catalog(
        db_file, catalog=TON_ORIGIN, code=MAIN_RECORD_CODE,
        period=0, description='Справочник нормативов ТСН'
    )
    insert_root_record_to_catalog(
        db_file, catalog=PNWC_ORIGIN, code=MAIN_RECORD_CODE,
        period=0, description='Справочник ресурсов НЦКР'
    )
    return 0


if __name__ == '__main__':
    from config import LocalData

    location = "office"  # office  # home
    local = LocalData(location)

    db_create_tables_and_fill_directory(local.db_file)
