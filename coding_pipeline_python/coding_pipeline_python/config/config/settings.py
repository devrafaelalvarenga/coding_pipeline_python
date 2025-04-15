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
    """Função responsável pela criação da URL de acesso a API da Alpha Vantage que retorna séries temporais intradiárias

    Args:
        symbol (_type_, optional): Nome da moeda a ser consultada. Padrão: DEFAULT_SYMBOL.
        interval (_type_, optional): Intervalo de tempo entre dois pontos de dados consecutivos na série temporal. Os seguintes valores são suportados: 1min, 5min, 15min, 30min,60min. Padrão: DEFAULT_INTERVAL.
        apikey (_type_, optional): Chave da API. Padrão: API_KEY.

    Returns:
        _type_: URL de acesso a API da Alpha Vantage que retorna séries temporais intradiárias - TIME_SERIES_INTRADAY
    """
    return f'{BASE_URL}?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={apikey}'
