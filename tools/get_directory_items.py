import pandas as pd


from config import dbTolls, DirectoryItem
from sql_queries import sql_items_queries


def get_sorted_directory_items(db_filename: str, directory_name: str) -> tuple[DirectoryItem] | None:
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
        dir_tree.append(DirectoryItem(id=top_id, item_name=top_item[1], directory_name=parameters[0]))
        item_df = df[df['ID_parent'] == top_id]
        while not item_df.empty:
            item_data = item_df.to_records(index=False).tolist()[0]
            next_id = item_data[0]
            dir_tree.append(DirectoryItem(id=next_id, item_name=item_data[1], directory_name=directory_name))
            item_df = df[df['ID_parent'] == next_id]
        return tuple(dir_tree)

    df = df.sort_values(by='ID_tblItem', ascending=True)
    return tuple([DirectoryItem(row.ID_tblItem, row.name, row.team) for row in df.itertuples(index=False)])


if __name__ == '__main__':
    import os
    from icecream import ic

    # db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
    db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
    db_name = os.path.join(db_path, "Normative.sqlite3")

    # directory = get_sorted_directory_items(db_name, directory_name='machines')
    # ic(directory)

    directory = get_sorted_directory_items(db_name, directory_name='units')
    ic(directory)
