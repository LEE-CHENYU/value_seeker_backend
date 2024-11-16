import requests
import pandas as pd
import sqlite3
import unittest
from unittest.mock import patch

# TickerTick API configuration
base_url = 'https://api.tickertick.com/feed'
symbol = 'OXY'  # Example symbol

def fetch_and_store_tickertick_news():
    headers = {
        'Content-Type': 'application/json'
    }
    
    params = {
        'symbol': symbol,
        'limit': 1000  # Number of news items to fetch
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data['stories'])
        
        # Connect to SQLite database
        conn = sqlite3.connect('tickertick_news.db')
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickertick_news (
                id TEXT PRIMARY KEY,
                title TEXT,
                url TEXT UNIQUE,
                site TEXT,
                time INTEGER,
                favicon_url TEXT,
                tags TEXT,
                description TEXT,
                tickers TEXT
            )
        ''')
        
        # Insert data
        for _, row in df.iterrows():
            cursor.execute('''
                INSERT OR REPLACE INTO tickertick_news
                (id, title, url, site, time, favicon_url, tags, description, tickers)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row.get('id'),
                row.get('title'),
                row.get('url'),
                row.get('site'),
                row.get('time'),
                row.get('favicon_url'),
                str(row.get('tags', [])),
                row.get('description'),
                str(row.get('tickers', []))
            ))
            
        conn.commit()
        conn.close()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from TickerTick API: {e}")
        raise

class TestTickerTickNewsFetcher(unittest.TestCase):
    def setUp(self):
        # Make an actual API call to get real data
        headers = {'Content-Type': 'application/json'}
        params = {'symbol': symbol, 'limit': 1}
        response = requests.get(base_url, headers=headers, params=params)
        self.real_data = response.json()
        print("Real data:")
        print(self.real_data)
        
        self.test_entry = self.real_data['stories'][0] if self.real_data else None
        print("Test entry:")
        print(self.test_entry)

    def test_fetch_and_store_tickertick_news(self):
        if not self.test_entry:
            self.skipTest("No data received from API")

        # Run the function with real API data
        fetch_and_store_tickertick_news()
        
        # Verify data was stored correctly
        conn = sqlite3.connect('tickertick_news.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tickertick_news WHERE url = ?', 
                     (self.test_entry['url'],))
        result = cursor.fetchone()
        conn.close()
        
        self.assertIsNotNone(result, "Test entry not found in the database")
        if result:
            self.assertEqual(result[0], self.test_entry.get('id', None))
            self.assertEqual(result[1], self.test_entry.get('title', None))
            self.assertEqual(result[2], self.test_entry.get('url', None))
            self.assertEqual(result[3], self.test_entry.get('site', None))
            self.assertEqual(result[4], self.test_entry.get('time', None))
            self.assertEqual(result[5], self.test_entry.get('favicon_url', None))
            self.assertEqual(result[6], str(self.test_entry.get('tags', [])))
            self.assertEqual(result[7], self.test_entry.get('description', None))
            self.assertEqual(result[8], str(self.test_entry.get('tickers', [])))

if __name__ == '__main__':
    unittest.main()
