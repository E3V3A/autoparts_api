import os
import sqlite3


class SqlLiteTempDb(object):
    def __init__(self):
        pass

    def __enter__(self):
        self.delete_sql_lite_db()
        sqlite_file = 'sql_lite_db'
        sql = sqlite3.connect(sqlite_file)
        sql.isolation_level = None
        sql.row_factory = sqlite3.Row
        self.sql_cursor = sql.cursor()
        return self.sql_cursor

    def __exit__(self, *args):
        self.sql_cursor.close()
        self.delete_sql_lite_db()

    def delete_sql_lite_db(self):
        try:
            os.remove("sql_lite_db")
        except:
            pass