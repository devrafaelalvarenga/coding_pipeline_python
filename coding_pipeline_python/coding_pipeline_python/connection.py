import sqlite3
import datetime
import requests
import json as json
import pandas as pd
# coding: utf-8

database = 'alphavantage.db'
source_table = 'raw_time_series_intraday'
target_table = 'standard_series_intraday'
current_time = datetime.datetime.now()

# Replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'

# Register adapters and converters for datetime
sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "DATETIME", lambda s: datetime.datetime.fromisoformat(s.decode("utf-8")))


class DBConnection():
    def __init__(self):
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


class RawDataExtractor(DBConnection):
    def __init__(self):
        DBConnection.__init__(self)

    def get_data(self, url):
        try:
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                return data
            else:
                return ('Error:', data['Error Message'])
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


class RawDataSource(DBConnection):
    def __init__(self):
        DBConnection.__init__(self)

    def create_table(self):
        try:
            sql = f'''
                CREATE TABLE IF NOT EXISTS {source_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extract_data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    raw_data TEXT

                )
            '''
            self.execute(sql)
            self.commit()
        except sqlite3.Error as e:
            print('Erro na criação da tabela:', e)

    def insert(self, data):
        try:
            sql = f'''
            INSERT INTO {source_table} (extract_data, raw_data)
            VALUES (?, ?)
            '''
            self.execute(sql, (current_time, json.dumps(data)))
            self.commit()
        except sqlite3.Error as e:
            print('Erro na inserção:', e)

    def delete(self):
        try:
            sql = f'''
                DELETE FROM {source_table}
            '''
            self.execute(sql)
            self.commit()
        except sqlite3.Error as e:
            print('Erro na deleção:', e)

    def drop(self):
        try:
            sql = f'''
                DROP TABLE IF EXISTS {source_table}
            '''
            self.execute(sql)
            self.commit()
        except sqlite3.Error as e:
            print('Erro na deleção:', e)


class RawDataProcessor(DBConnection):
    def __init__(self):
        DBConnection.__init__(self)

    def normalize_raw_table(self, source_table, target_table):
        try:
            sql_v = f'''
                SELECT name FROM sqlite_master WHERE type='table' AND name={target_table}
            '''
            self.execute(sql_v)
            if self.fetchall():
                print(f"Tabela '{target_table}' já existe.")
                exit(1)

            sql = f'''
                SELECT raw_data FROM {source_table}
            '''
            self.execute(sql)
            linhas = self.fetchall()

            todos_df = []

            for (raw_json,) in linhas:
                dados = json.loads(raw_json)

                meta = dados.get("Meta Data", {})
                series = dados.get("Time Series (5min)", {})
                symbol = meta.get("2. Symbol", "N/A")
                timezone = meta.get("6. Time Zone", "UTC")

                df = pd.DataFrame.from_dict(series, orient='index')
                df.columns = ["open", "high", "low", "close", "volume"]
                df.index.name = "timestamp"  # renomeia o índice
                df.reset_index(inplace=True)

                df[["open", "high", "low", "close"]] = df[[
                    "open", "high", "low", "close"]].astype(float)
                df["volume"] = df["volume"].astype(int)
                df["symbol"] = symbol
                df["timezone"] = timezone

                todos_df.append(df)

            if todos_df:
                final_df = pd.concat(todos_df, ignore_index=True)
                final_df.to_sql(target_table, self.conn,
                                if_exists="replace", index=False)
                print(
                    f"Tabela '{target_table}' criada com {len(final_df)} registros.")
            else:
                print("Nenhum dado bruto encontrado para normalizar.")

        except Exception as e:
            print("Erro ao normalizar dados:", e)


if __name__ == '__main__':
    with RawDataExtractor() as raw_data_extractor:
        data = raw_data_extractor.get_data(url)

    with RawDataSource() as raw_data_source:
        raw_data_source.create_table()
        raw_data_source.insert(data)
        # raw_data_source.delete()
        # raw_data_source.drop()

    with RawDataProcessor() as raw_data_processor:
        raw_data_processor.normalize_raw_table(source_table, target_table)
        # print(data)
