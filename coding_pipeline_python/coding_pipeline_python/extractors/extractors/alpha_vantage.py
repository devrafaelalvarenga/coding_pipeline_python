import requests
import json
import sqlite3
from config.config.settings import CURRENT_TIME
from db.db.connection import DBConnection


class RawDataExtractor(DBConnection):
    def __init__(self):
        super().__init__()

    def get_data(self, url):
        """Extrai dados da API Alpha Vantage"""
        try:
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                return data
            else:
                return ('Error:', data.get('Error Message', 'Unknown error'))
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


class RawDataSource(DBConnection):
    def __init__(self, table_name):
        super().__init__()
        self.table_name = table_name

    def create_table(self):
        """Cria a tabela para armazenar dados brutos"""
        try:
            sql = f'''
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extract_data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    raw_data TEXT
                )
            '''
            self.execute(sql)
            self.commit()
            print(f"Tabela '{self.table_name}' criada ou já existente.")
        except sqlite3.Error as e:
            print('Erro na criação da tabela:', e)

    def insert(self, data):
        """Insere dados brutos na tabela"""
        try:
            sql = f'''
            INSERT INTO {self.table_name} (extract_data, raw_data)
            VALUES (?, ?)
            '''
            self.execute(sql, (CURRENT_TIME, json.dumps(data)))
            self.commit()
            print(f"Dados inseridos na tabela '{self.table_name}'")
        except sqlite3.Error as e:
            print('Erro na inserção:', e)

    def delete(self):
        """Deleta todos os dados da tabela"""
        try:
            sql = f'''
                DELETE FROM {self.table_name}
            '''
            self.execute(sql)
            self.commit()
            print(
                f"Todos os dados da tabela '{self.table_name}' foram deletados.")
        except sqlite3.Error as e:
            print('Erro na deleção:', e)

    def drop(self):
        """Remove a tabela do banco de dados"""
        try:
            sql = f'''
                DROP TABLE IF EXISTS {self.table_name}
            '''
            self.execute(sql)
            self.commit()
            print(f"Tabela '{self.table_name}' removida.")
        except sqlite3.Error as e:
            print('Erro na remoção da tabela:', e)
