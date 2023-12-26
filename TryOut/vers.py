import os
import sys
import sqlite3


if __name__ == '__main__':
    version = f"SQLite: {sqlite3.sqlite_version}\nPython: {sys.version}\n {sys.path}"
    print(version)
