import sqlite3
import datetime

database_name = 'alphavantage.db'
current_time = datetime.datetime.now()

# Register adapters and converters for datetime
sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "DATETIME", lambda s: datetime.datetime.fromisoformat(s.decode("utf-8")))


class Connection():
    def __init__(self):
        try:
            self.conn = sqlite3.connect(database_name)
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


class ExtractData(Connection):
    def __init__(self):
        Connection.__init__(self)

    def get_data(self, url):
        import requests
        import json
        # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'
        try:
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                return data
            else:
                return ('Error:', data['Error Message'])
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


class RawData(Connection):
    def __init__(self):
        Connection.__init__(self)

    def create_table(self):
        try:
            sql_create = '''
                CREATE TABLE IF NOT EXISTS raw_time_series_intraday (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extract_data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    raw_data TEXT
                )
            '''
            self.execute(sql_create)
            self.commit()
        except sqlite3.Error as e:
            print('Erro na criação da tabela:', e)

    def insert(self, data):
        try:
            import json
            sql_insert = '''
            INSERT INTO raw_time_series_intraday (extract_data, raw_data)
            VALUES (?, ?)
            '''
            self.execute(sql_insert, (current_time, json.dumps(data)))
            self.commit()
        except sqlite3.Error as e:
            print('Erro na inserção:', e)

    def delete(self):
        try:
            sql_delete = '''
                DELETE FROM raw_time_series_intraday
            '''
            self.execute(sql_delete)
            self.commit()
        except sqlite3.Error as e:
            print('Erro na deleção:', e)

    def drop(self):
        try:
            sql_drop = '''
                DROP TABLE IF EXISTS raw_time_series_intraday
            '''
            self.execute(sql_drop)
            self.commit()
        except sqlite3.Error as e:
            print('Erro na deleção:', e)


if __name__ == '__main__':
    with ExtractData() as extract_data:
        data = extract_data.get_data(
            'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo')
        print(data)

    with RawData() as raw_data:
        raw_data.insert(data)
        # raw_data.delete()
        # raw_data.create_table()
        # raw_data.drop()
