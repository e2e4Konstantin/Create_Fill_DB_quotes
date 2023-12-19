import os
import sqlite3
from icecream import ic

# db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
db_name = os.path.join(db_path, "Normative.sqlite3")

try:
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    fid_list = ['main', 'quotes']
    fid_tuple = tuple(fid_list)
    fmt = format(fid_tuple)
    f2 = fid_tuple.__str__()
    f3 = fid_tuple.__repr__()

    f1 = f"SELECT * FROM tblItems WHERE team IN {fid_tuple.__str__()}"
    ic(f1)
    # ic(f_query)
    #
    f2 = "SELECT * FROM tblItems WHERE team IN ('main', 'quotes');"
    ic(f2)

    ic(f1 == f2)

    # f3 = """SELECT ID_tblItem, name, title, ID_parent, re_pattern FROM tblItems WHERE team IN ?;"""

    #
    results = cur.execute(f1)
    r = list(results.fetchall())
    ic(r)

    conn.close()
except Exception as e:
    print(f'An error occurred: {e}.')
    exit()
