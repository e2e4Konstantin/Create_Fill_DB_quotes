import pandas as pd

from config import dbTolls, DirectoryItem
from sql_queries import sql_items_queries, sql_catalog_queries
from files_features import output_message, output_message_exit


def get_catalog_id_by_code(db: dbTolls, code: str) -> int | None:
    """ Ищет в Каталоге tblCatalogs запись по шифру. """
    row_id = db.get_row_id(sql_catalog_queries["select_catalog_id_code"], (code,))
    if row_id is None:
        output_message(f"В каталоге не найдена запись:", f"шифр: {code!r}")
    return row_id


def get_sorted_directory_items(db_filename: str, directory_name: str) -> tuple[DirectoryItem, ...] | None:
    """ Формирует упорядоченный кортеж (id, name) из элементов справочника directory_name
        в соответствии с иерархией заданной полем ID_parent. Если это поле не задано,
        то берутся все элементы справочника последовательно.
        Первым элементом добавляется элементы группы 'main'.
        """
    # названия групп которые надо отфильтровать, первым вставляется головной элемент
    directory_name = directory_name.lower()
    parameters = ['main', directory_name]
    with (dbTolls(db_filename) as db):
        df = pd.read_sql_query(sql=sql_items_queries["select_items_dual_teams"], params=parameters, con=db.connection)
    if df.empty:
        return None
    columns = ['ID_tblItem', 'ID_parent']
    df[columns] = df[columns].astype(pd.Int8Dtype())
    # Есть только один элемент у которого нет родителя, справочник иерархический
    if df['ID_parent'].isna().sum() == 1:
        dir_tree: list[DirectoryItem] = []
        top_item = df[df['ID_parent'].isna()].to_records(index=False).tolist()[0]
        top_id = top_item[0]
        dir_tree.append(DirectoryItem(
            id=top_id, item_name=top_item[2], directory_name=parameters[0], re_pattern=top_item[5])
        )
        item_df = df[df['ID_parent'] == top_id]
        while not item_df.empty:
            item_data = item_df.to_records(index=False).tolist()[0]
            next_id = item_data[0]
            dir_tree.append(
                DirectoryItem(id=next_id, item_name=item_data[2], directory_name=item_data[1], re_pattern=item_data[5])
            )
            item_df = df[df['ID_parent'] == next_id]
        return tuple(dir_tree)

    df = df.drop(df.index[0])
    df = df.sort_values(by='name', ascending=True)
    return tuple([DirectoryItem(row[0], row[2], row[1], row[5]) for row in df.itertuples(index=False)])


def delete_catalog_rows_with_old_period(db_filename: str):
    """ Поучает максимальный период из всех записей таблицы каталога.
        Обновляет период у головной записи на максимальный.
        Удаляет все записи у которых период < максимального. """
    with (dbTolls(db_filename) as db):
        work_cursor = db.go_execute(sql_catalog_queries["select_catalog_max_period"])
        max_period = work_cursor.fetchone()['max_period'] if work_cursor else None
        if max_period is None:
            output_message_exit(f"при получении максимального периода Каталога", f"{max_period=}")
            return
        ic(max_period)

        db.go_execute(sql_catalog_queries["update_catalog_period_main_row"], (max_period,))
        changes = db.go_execute(sql_catalog_queries["select_changes"]).fetchone()['changes']
        message = f"обновлен период {max_period} головной записи: {changes}"
        ic(message)

        deleted_cursor = db.go_execute(sql_catalog_queries["select_count_last_period"], (max_period,))
        number_past = deleted_cursor.fetchone()[0]
        mess = f"Из Каталога будут удалены {number_past} записей у которых период меньше текущего: {max_period}"
        ic(mess)
        #
        deleted_cursor = db.go_execute(sql_catalog_queries["delete_catalog_last_periods"], (max_period,))
        mess = f"Из Каталога удалено {deleted_cursor.rowcount} записей с period < {max_period}"
        ic(mess)





if __name__ == '__main__':
    import os
    from icecream import ic

    db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    # db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    directory = get_sorted_directory_items(db_name, directory_name='machines')
    ic(directory)

    directory = get_sorted_directory_items(db_name, directory_name='units')
    ic(directory)

    directory = get_sorted_directory_items(db_name, directory_name='quotes')
    ic(directory)
