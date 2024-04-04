import sqlite3
import re
db_path = r"C:\Users\kazak.ke\Documents\PythonProjects\DB\Normative.sqlite3"

queries = {
    "select_all": """--sql
        SELECT * FROM tblProducts;
    """,
    "select_products_max_supplement_origin_item": """--sql
        SELECT MAX(per.supplement_num) AS max_suppl
        FROM tblProducts AS m
        JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblProducts_tblPeriods
        WHERE m.FK_tblProducts_tblOrigins = ? AND m.FK_tblProducts_tblItems = ?;
    """,
    "update_product_id": """--sql
        UPDATE tblProducts
        SET
            FK_tblProducts_tblCatalogs = ?, FK_tblProducts_tblItems = ?,
            FK_tblProducts_tblOrigins = ?, FK_tblProducts_tblPeriods = ?,
            code = ?, description = ?, measurer = ?, digit_code = ?
        WHERE ID_tblProduct = ?;
    """,
    "select_raw_items_by_chapter": """--sql
        /* два параметра 1: '(^\s*1\..*)|(^\s*1\s*$)' 2: '^\s*(\d+)\s*$' */
        SELECT
        (SELECT p.pressmark FROM tblRawData AS p WHERE p.id = CAST(m.parent_id AS INT) ) AS [parent_pressmark], m.*, m.*
        FROM (SELECT f.* FROM tblRawData f WHERE f.pressmark REGEXP ?) AS m
        WHERE m.pressmark REGEXP ?;
    """,
}

data = (1860, 2, 1, 66, '3.0-0-1',
        '######### Затраты на превышение стоимости электроэнергии, получаемой от передвижных электростанций', '1 кВт.-ч.', 3000000001000000)

ID_tblProduct = 1

x = data + (ID_tblProduct, )
print(x)




def regex(expression, item):
    reg = re.compile(expression)
    return reg.search(item) is not None


if __name__ == "__main__":
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.create_function("REGEXP", 2, regex)


    # result = conn.execute(queries["select_all"])
    # if result:
    #     row = result.fetchone()
    #     print(row['code'], row['description'])

    # result = self.connection.execute(query, *args)
    pars = ('(^\s*1\..*)|(^\s*1\s*$)', '^\s*(\d+)\s*$')
    print(pars)
    result = conn.execute(queries["select_raw_items_by_chapter"], pars)
    if result:
        row = result.fetchone()
        print(dict(row))

    # result = conn.execute(
    #     queries["update_product_id"], x)


    if conn is not None:
        conn.commit()
        conn.close()
