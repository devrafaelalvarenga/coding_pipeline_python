import sqlite3
import datetime
from config.config.settings import DATABASE

# Registrando adaptadores e conversores para data e hora
sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "DATETIME", lambda s: datetime.datetime.fromisoformat(s.decode("utf-8")))


class DBConnection:
    def __init__(self, database=DATABASE):
        try:
            self.conn = sqlite3.connect(database)
            self.cur = self.conn.cursor()
        except sqlite3.Error as e:
            print('Erro na conexão:', e)
            exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.commit()
        self.conn.close()

    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.conn.commit()

    def fetchall(self):
        return self.cur.fetchall()

    def execute(self, query, params=None):
        if params:
            self.cur.execute(query, params or ())
        else:
            self.cur.execute(query)

    def query(self, query, params=None):
        self.execute(query, params)
        return self.fetchall()
