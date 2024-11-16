import json
import requests

# # data = json.load(requests.get('https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey=AYTLT9XYXR8L9OSZ'))
# # print(data)
# with requests.get('https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey=AYTLT9XYXR8L9OSZ') as f: 
#     json.load(f)

params = {
    'function': 'NEWS_SENTIMENT',
    'tickers': 'AAPL',
    'apikey': 'LGOQT2OT2BFJSW5O'
}

url = 'https://www.alphavantage.co/query'

data = requests.get(url, params=params)
print(data.json())
for key, value in data.json().items():
    print(key, value)

