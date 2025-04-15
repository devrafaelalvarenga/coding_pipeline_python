import sqlite3
import datetime
from config.config.settings import DATABASE

# Registrando adaptadores e conversores para data e hora
sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "DATETIME", lambda s: datetime.datetime.fromisoformat(s.decode("utf-8")))


class DBConnection:
    """A classe DBConnection contém funções ligadas ao Banco de dados

    Returns:
        Funções ligadas a transações com o Banco de dados
    """

    def __init__(self, database=DATABASE):
        """_summary_

        Args:
            database (_type_, optional): _description_. Defaults to DATABASE.

        Returns:
            _type_: _description_
        """
        try:
            self.conn = sqlite3.connect(database)
            self.cur = self.conn.cursor()
        except sqlite3.Error as e:
            print('Erro na conexão:', e)
            exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """_summary_

        Args:
            exc_type (_type_): _description_
            exc_value (_type_): _description_
            traceback (_type_): _description_

        Returns:
            _type_: _description_
        """
        self.commit()
        self.conn.close()

    @property
    def connection(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.conn

    @property
    def cursor(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.cur

    def commit(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        self.conn.commit()

    def fetchall(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.cur.fetchall()

    def execute(self, query, params=None):
        """_summary_

        Args:
            query (_type_): _description_
            params (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        if params:
            self.cur.execute(query, params or ())
        else:
            self.cur.execute(query)

    def query(self, query, params=None):
        """_summary_

        Args:
            query (_type_): _description_
            params (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        self.execute(query, params)
        return self.fetchall()
