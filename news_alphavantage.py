import requests
import pandas as pd
import sqlite3
import unittest
from unittest.mock import patch

# Original code (unchanged)
function = 'NEWS_SENTIMENT'
tickers = 'OXY'
api_key = 'AYTLT9XYXR8L9OSZ'
url = f'https://www.alphavantage.co/query?function={function}&tickers={tickers}&apikey={api_key}'

def fetch_and_store_news():
    r = requests.get(url)
    data = r.json()
    feed_data = data.get('feed', [])
    df = pd.DataFrame(feed_data)

    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news (
            title TEXT,
            url TEXT,
            time_published TEXT,
            authors TEXT,
            summary TEXT,
            source TEXT,
            overall_sentiment_score REAL,
            overall_sentiment_label TEXT,
            ticker_sentiment TEXT,
            topics TEXT,
            UNIQUE(url)
        )
    ''')

    for _, row in df.iterrows():
        cursor.execute('''
            INSERT OR REPLACE INTO news 
            (title, url, time_published, authors, summary, source, 
             overall_sentiment_score, overall_sentiment_label, ticker_sentiment, topics)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.get('title'),
            row.get('url'),
            row.get('time_published'),
            str(row.get('authors', [])),
            row.get('summary'),
            row.get('source'),
            row.get('overall_sentiment_score'),
            row.get('overall_sentiment_label'),
            str(row.get('ticker_sentiment', [])),
            str(row.get('topics', []))
        ))

    conn.commit()
    conn.close()
class TestNewsFetcher(unittest.TestCase):
    def setUp(self):
        # Make an actual API call to get real data
        r = requests.get(url)
        self.real_data = r.json()
        print(self.real_data)

    def test_fetch_and_store_news(self):
        # Use the first entry from the real API response
        test_entry = self.real_data['feed'][0]

        with patch('requests.get') as mock_get:
            mock_response = {'feed': [test_entry]}
            mock_get.return_value.json.return_value = mock_response

            # Run the function
            fetch_and_store_news()

            # Check if the data was stored correctly
            conn = sqlite3.connect('news.db')
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM news WHERE url = ?', (test_entry['url'],))
            result = cursor.fetchone()
            conn.close()

            self.assertIsNotNone(result, "Test entry not found in the database")
            if result:
                self.assertEqual(result[0], test_entry['title'])
                self.assertEqual(result[1], test_entry['url'])
                self.assertEqual(result[2], test_entry['time_published'])
                self.assertEqual(result[3], str(test_entry.get('authors', [])))
                self.assertEqual(result[4], test_entry['summary'])
                self.assertEqual(result[5], test_entry['source'])
                self.assertEqual(result[6], test_entry['overall_sentiment_score'])
                self.assertEqual(result[7], test_entry['overall_sentiment_label'])
                self.assertEqual(result[8], str(test_entry.get('ticker_sentiment', [])))
                self.assertEqual(result[9], str(test_entry.get('topics', [])))

if __name__ == '__main__':
    unittest.main()