import datetime

# Configurações de Banco de Dados
DATABASE = 'alphavantage.db'
SOURCE_TABLE = 'raw_time_series_intraday'
TARGET_TABLE = 'standard_series_intraday'
CURRENT_TIME = datetime.datetime.now()

# Configurações da API
API_KEY = 'demo'
BASE_URL = 'https://www.alphavantage.co/query'
DEFAULT_SYMBOL = 'IBM'
DEFAULT_INTERVAL = '5min'

# Construção da URL


def get_intraday_url(symbol=DEFAULT_SYMBOL, interval=DEFAULT_INTERVAL, apikey=API_KEY):
    return f'{BASE_URL}?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={apikey}'
