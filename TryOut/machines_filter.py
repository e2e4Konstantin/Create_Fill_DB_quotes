import os
import sqlite3
from icecream import ic
import re

db_path = r"F:\Kazak\GoogleDrive\Python_projects\DB"
# db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB"
db_name = os.path.join(db_path, "Normative.sqlite3")


def regex(expression, item):
    reg = re.compile(expression)
    return reg.search(item) is not None

try:
    conn = sqlite3.connect(db_name)
    conn.create_function("REGEXP", 2, regex)
    cur = conn.cursor()

    r = r"^\s*((\d+)\.(\d+))\s*$"
    q = """
        SELECT ID, PARENT, CMT, TITLE, PERIOD 
        FROM tblRawData 
        WHERE ("Ед.Изм." IS NULL OR "Брутто" IS NULL) AND CMT REGEXP ?;        
    """

    #
    results = cur.execute(q, (r,))
    r = list(results.fetchall())
    ic(r)

    conn.close()
except Exception as e:
    print(f'An error occurred: {e}.')
    exit()
