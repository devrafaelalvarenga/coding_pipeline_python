import requests
import json

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'

try:
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        print(data)
    else:
        print('Error:', data['Error Message'])
except requests.exceptions.RequestException as e:
    raise SystemExit(e)
