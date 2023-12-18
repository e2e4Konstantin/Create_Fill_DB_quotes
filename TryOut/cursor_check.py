from itertools import chain

results = cursor.execute('SELECT ID, text FROM mytable')
try:
    first_row = next(results)
    for row in chain((first_row,), results):
        pass # do something
except StopIteration as e:
    pass # 0 results


first_row = next(cursor, [None])[0]


def yield_data(cursor):
    while True:
        if cursor.description is None:
            # No recordset for INSERT, UPDATE, CREATE, etc
            pass
        else:
            # Recordset for SELECT, yield data
            yield cursor.fetchall()
            # Or yield column names with
            # yield [col[0] for col in cursor.description]

        # Go to the next recordset
        if not cursor.nextset():
            # End of recordsets
            return