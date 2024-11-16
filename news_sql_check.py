import sqlite3
from datetime import datetime

def get_oldest_tickertick_news():
    conn = sqlite3.connect('tickertick_news.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT title, url, time 
        FROM tickertick_news 
        ORDER BY time ASC 
        LIMIT 1
    ''')
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'title': result[0],
            'url': result[1], 
            'timestamp': result[2]
        }
    return None

if __name__ == '__main__':
    oldest = get_oldest_tickertick_news()
    if oldest:
        print(f"Oldest article:")
        print(f"Title: {oldest['title']}")
        print(f"URL: {oldest['url']}")
        try:
            print(f"Timestamp: {datetime.fromtimestamp(oldest['timestamp'])}")
        except ValueError as e:
            print(f"Error converting timestamp: {e}")
            print(f"Raw timestamp value: {oldest['timestamp']}")
    else:
        print("No articles found in database")
