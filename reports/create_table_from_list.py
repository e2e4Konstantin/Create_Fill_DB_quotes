
# table_name = "my_table"
# columns = ["column1", "column2", "column3"]

# # Create the table
# create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{column} TEXT' for column in columns])})"
# cursor.execute(create_table_query)

# # Define the list of dictionaries
# data = [
#     {"column1": "value1", "column2": "value2", "column3": "value3"},
#     {"column1": "value4", "column2": "value5", "column3": "value6"},
#     # Add more dictionaries as needed
# ]

# # Insert the data into the table
# insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])})"
# cursor.executemany(insert_query, data)

# # Commit the changes and close the connection
# conn.commit()
# conn.close()

# select_query = "SELECT column1, column2 FROM my_table WHERE condition"
# cursor.execute(select_query)

# # Fetch all rows from the SELECT query
# rows = cursor.fetchall()


sql_slice_materials = {
    "create_table_slice_materials": """--sql
        CREATE TABLE tblSliceMaterials (
            product_rowid INTEGER PRIMARY KEY,
            product_id INTEGER,
            product_period_id INTEGER,
            product_code TEXT,
            product_description TEXT,
            product_measurer TEXT,
            product_digit_code INTEGER,
            catalog_id INTEGER,
            catalog_code TEXT,
            UNIQUE (product_period_id, product_id)
        );
    """,
    "create_index_slice_materials": """--sql
        CREATE INDEX idxSliceMaterials ON tblSliceMaterials (product_id, product_period_id);
    """,
    "insert_slice_materials": """--sql
        INSERT INTO tblSliceMaterials (
                product_rowid, product_id, product_period_id,
                product_code, product_description, product_measurer,
                product_digit_code, catalog_id, catalog_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
}



"""
cursor.executemany(insert_query, rows)


