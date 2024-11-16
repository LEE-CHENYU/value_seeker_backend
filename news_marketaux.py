import requests
import pandas as pd
import sqlite3
import unittest
from unittest.mock import patch

# MarketAux API configuration
base_url = 'https://api.marketaux.com/v1/news/all'
api_key = 'LoidGJTwyGUKeMKCtv9IsJlJe3inl9OMOKMBRXtS'  # Replace with actual API key
symbols = 'OXY'  # Example symbol

def fetch_and_store_marketaux_news():
    params = {
        'api_token': api_key,
        'symbols': symbols,
        'limit': 100  # Number of news items to fetch
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data['data'])
        
        # Connect to SQLite database
        conn = sqlite3.connect('marketaux_news.db')
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS marketaux_news (
                uuid TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                snippet TEXT,
                url TEXT UNIQUE,
                published_at TEXT,
                source TEXT,
                relevance_score REAL,
                entities TEXT,
                similar TEXT
            )
        ''')
        
        # Insert data
        for _, row in df.iterrows():
            cursor.execute('''
                INSERT OR REPLACE INTO marketaux_news
                (uuid, title, description, snippet, url, published_at, source, 
                 relevance_score, entities, similar)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row.get('uuid'),
                row.get('title'),
                row.get('description'),
                row.get('snippet'),
                row.get('url'),
                row.get('published_at'),
                row.get('source'),
                row.get('relevance_score'),
                str(row.get('entities', [])),
                str(row.get('similar', []))
            ))
            
        conn.commit()
        conn.close()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from MarketAux API: {e}")
        raise

class TestMarketAuxNewsFetcher(unittest.TestCase):
    def setUp(self):
        # Make an actual API call to get real data
        params = {
            'api_token': api_key,
            'symbols': symbols,
            'limit': 1
        }
        response = requests.get(base_url, params=params)
        self.real_data = response.json()
        print("Real data:")
        print(self.real_data)
        
        self.test_entry = self.real_data['data'][0] if self.real_data['data'] else None
        print("Test entry:")
        print(self.test_entry)

    def test_fetch_and_store_marketaux_news(self):
        if not self.test_entry:
            self.skipTest("No data received from API")

        # Run the function with real API data
        fetch_and_store_marketaux_news()
        
        # Verify data was stored correctly
        conn = sqlite3.connect('marketaux_news.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM marketaux_news WHERE url = ?', 
                     (self.test_entry['url'],))
        result = cursor.fetchone()
        conn.close()
        
        self.assertIsNotNone(result, "Test entry not found in the database")
        if result:
            self.assertEqual(result[0], self.test_entry.get('uuid'))
            self.assertEqual(result[1], self.test_entry.get('title'))
            self.assertEqual(result[2], self.test_entry.get('description'))
            self.assertEqual(result[3], self.test_entry.get('snippet'))
            self.assertEqual(result[4], self.test_entry.get('url'))
            self.assertEqual(result[5], self.test_entry.get('published_at'))
            self.assertEqual(result[6], self.test_entry.get('source'))
            self.assertEqual(result[7], self.test_entry.get('relevance_score'))
            self.assertEqual(result[8], str(self.test_entry.get('entities', [])))
            self.assertEqual(result[9], str(self.test_entry.get('similar', [])))

if __name__ == '__main__':
    unittest.main()
