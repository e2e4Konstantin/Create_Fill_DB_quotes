import sqlite3
import pandas as pd


users = pd.read_csv('users.csv')

conn = sqlite3.connect('my_data.db')
c = conn.cursor()
users.to_sql('users', conn, if_exists='append', index = False, chunksize = 10000)
