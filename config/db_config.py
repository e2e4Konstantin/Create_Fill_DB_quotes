import sqlite3
import re
import os
# from itertools import chain
from icecream import ic

from files_features import output_message_exit, output_message


class dbControl:
    """ Класс для управления соединением БД. """

    def __init__(self, db_file_name: str):
        self.path = db_file_name
        self.connection = None
        self.connect()

    @staticmethod
    def regex(expression, item):
        reg = re.compile(expression)
        return reg.search(item) is not None

    def connect(self):
        try:
            self.connection = sqlite3.connect(self.path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            self.connection.create_function("REGEXP", 2, self.regex)
        except sqlite3.Error as err:
            self.close(err)
            output_message_exit(f"ошибка открытия БД Sqlite3: {err}", f"{self.path}")

    def close(self, exception_value=None):
        if self.connection is not None:
            if isinstance(exception_value, Exception):
                self.connection.rollback()
            else:
                self.connection.commit()
            self.connection.close()


class dbTolls(dbControl):
    """ Класс для работы с БД. """

    def __init__(self, db_file_name: str):
        super().__init__(db_file_name)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close(exception_value)

    def __str__(self):
        return f"db name: {self.path}, connect: {self.connection}"

    def __del__(self):
        self.connection.close() if self.connection is not None else self.connection

    def get_row_id(self, query: str, src_data: tuple) -> int | None:
        """ Выбрать id записи по запросу """
        try:
            result = self.connection.execute(query, src_data)
            if result:
                row = result.fetchone()
                return row[0] if row else None
        except sqlite3.Error as error:
            output_message_exit(f"ошибка запроса к БД Sqlite3: {' '.join(error.args)}",
                                f"получить id записи {src_data}")
        return None

    def go_insert(self, query: str, src_data: tuple, message: str) -> int | None:
        """ Пытается выполнить запрос на вставку записи в БД. Возвращает rowid """
        try:
            result = self.connection.execute(query, src_data)
            if result:
                return result.lastrowid
        except sqlite3.Error as error:
            output_message(f"ошибка INSERT запроса БД Sqlite3: {' '.join(error.args)}", f"{message}")
        return None

    # def go_select_chain(self, query: str, src_data: tuple) -> list[sqlite3.Row] | None:
    #     """ Пытается выполнить запрос на выборку записей из БД. Возвращает список строк.  """
    #     try:
    #         results = self.connection.execute(query, src_data)
    #         try:
    #             first_row = next(results)
    #             return list(chain((first_row,), results))
    #         except StopIteration as e:
    #             return None
    #     except sqlite3.Error as error:
    #         output_message(f"ошибка SELECT запроса БД Sqlite3: {' '.join(error.args)}", f"{src_data}")
    #     return None

    def go_select(self, query: str, src_data: tuple = None) -> list[sqlite3.Row] | None:
        """ Пытается выполнить запрос на выборку записей из БД. Возвращает список строк. """
        try:
            if src_data:
                cursor = self.connection.execute(query, src_data)
            else:
                cursor = self.connection.execute(query)
            return cursor.fetchall() if cursor else None
        except sqlite3.Error as error:
            output_message_exit(f"ошибка SELECT запроса БД Sqlite3: {' '.join(error.args)}", f"{src_data}")
        return None

    def go_execute(self, *args, **kwargs) -> sqlite3.Cursor | None:
        try:
            result = self.connection.execute(*args, **kwargs)
            return result
        except sqlite3.Error as error:
            output_message(f"ошибка запроса БД Sqlite3: {' '.join(error.args)}", f"{args}")
            # print(f"SQLite error: {' '.join(error.args)}")
            # print(f"Exception class is: {error.__class__}")
            # print('SQLite traceback: ')
            # exc_type, exc_value, exc_tb = sys.exc_info()
            # print(traceback.format_exception(exc_type, exc_value, exc_tb))
            # print(error)

    def inform(self, all_details: bool = False):
        """  Выводи в консоль информацию о таблицах БД
        :param all_details: выводить все записи
        """
        if self.connection:
            with self.connection as db:
                cursor = self.connection.execute('SELECT SQLITE_VERSION()')
                print(f"SQLite version: {cursor.fetchone()[0]}")
                print(f"connect.total_changes: {db.total_changes}")

                cursor = self.connection.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                for index, table_i in enumerate(tables):
                    table_name = table_i[0]
                    count = self.connection.execute(f"SELECT COUNT(1) from {table_name}")
                    print(f"\n{index + 1}. таблица: {table_name}, записей: {count.fetchone()[0]}")
                    table_info = self.connection.execute(f"PRAGMA table_info({table_name})")
                    data = table_info.fetchall()

                    print(f"поля таблицы: ")
                    print([tuple(d) for d in data])
                    if all_details:
                        print(f"данные таблицы:")
                        cursor = self.connection.execute(f"SELECT * from {table_name}")
                        print([row_i for row_i in cursor])


if __name__ == '__main__':
    version = f"{sqlite3.version} {sqlite3.sqlite_version}"
    db_name = os.path.join(r"F:\Kazak\GoogleDrive\Python_projects\DB", "quotes_test.sqlite3")
    ic(version)
    ic(db_name)
    db = dbTolls(db_name)
    id = db.get_row_id("select ID_tblDirectoryItem from tblDirectoryItems where code = ?;", 'Table')
    ic(id)
    db.close()
    with dbTolls(db_name) as db:
        id = db.get_row_id("select ID_tblDirectoryItem from tblDirectoryItems where code = ?;", 'Section')
        ic(id)
        db.inform()
