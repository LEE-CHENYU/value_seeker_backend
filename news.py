import requests

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=OXY&apikey=AYTLT9XYXR8L9OSZ'
r = requests.get(url)
data = r.json()

import pandas as pd

# Convert feed data to DataFrame
feed_data = data.get('feed', [])
df = pd.DataFrame(feed_data)

# Display the DataFrame
print(df)