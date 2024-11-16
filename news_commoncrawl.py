import requests
import json
import gzip
from io import BytesIO
import concurrent.futures
import sqlite3
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("commoncrawl_debug.log"),
                        logging.StreamHandler()
                    ])

class CommonCrawlAPI:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2,  # Increased backoff factor for longer waits between retries
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        logging.info("Initialized CommonCrawlAPI instance with enhanced retry strategy.")
        self.latest_index = self.get_latest_index()
        if not self.latest_index:
            logging.error("Failed to retrieve the latest Common Crawl index.")
            raise ValueError("Latest Common Crawl index could not be determined.")

    def get_latest_index(self):
        """Fetches the latest Common Crawl index."""
        index_list_url = "https://index.commoncrawl.org/"
        try:
            response = self.session.get(index_list_url, timeout=10)
            response.raise_for_status()
            indices = response.text.strip().split('\n')
            # Assuming indices are sorted and the latest is the last one
            latest = indices[-1] if indices else None
            if latest:
                logging.info(f"Latest Common Crawl index determined: {latest}")
            else:
                logging.error("No indices found at the Common Crawl index list.")
            return latest
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching latest index from Common Crawl: {e}")
            return None

    def search_index(self, url_pattern):
        api_url = f"https://index.commoncrawl.org/{self.latest_index}-index"
        params = {
            'url': url_pattern,
            'output': 'json'
        }
        logging.debug(f"Searching index with pattern: {url_pattern}")

        try:
            response = self.session.get(api_url, params=params, timeout=30)  # Increased timeout
            response.raise_for_status()
            results = [json.loads(line) for line in response.text.strip().split('\n') if line]
            logging.info(f"Found {len(results)} results for pattern: {url_pattern}")
            return results
        except requests.exceptions.RequestException as e:
            logging.error(f"Error during search_index with pattern '{url_pattern}': {e}")
            return []
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding failed for pattern '{url_pattern}': {e}")
            return []

    def fetch_page_content(self, warc_path, offset, length):
        headers = {'Range': f'bytes={offset}-{offset+length-1}'}
        url = f'https://commoncrawl.s3.amazonaws.com/{warc_path}'
        logging.debug(f"Fetching page content from {url} with headers {headers}")

        try:
            response = self.session.get(url, headers=headers, timeout=30)  # Increased timeout
            response.raise_for_status()
            if response.status_code == 206:
                with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
                    content = gz.read()
                decoded_content = content.decode('utf-8', errors='ignore')
                logging.info(f"Successfully fetched and decoded content from {url}")
                return decoded_content
            else:
                logging.warning(f"Unexpected status code {response.status_code} for URL: {url}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching page content from {url}: {e}")
            return None
        except gzip.BadGzipFile as e:
            logging.error(f"Gzip decompression failed for URL {url}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching content from {url}: {e}")
            return None

def get_financial_domains():
    """Returns a list of financial domains to search for"""
    domains = [
        "reuters.com/markets/*",
        "bloomberg.com/news/*",
        "cnbc.com/markets/*",
        "finviz.com/*", 
        "tradingview.com/*",
        "seekingalpha.com/symbol/*",
        "finance.yahoo.com/quote/*",
        "marketwatch.com/investing/stock/*",
        "benzinga.com/*",
        "thestreet.com/*",
        "barrons.com/*",
        "fool.com/investing/stock/*"
    ]
    logging.info(f"Financial domains to search: {domains}")
    return domains

def process_pattern(api, pattern):
    logging.debug(f"Processing pattern: {pattern}")
    results = api.search_index(pattern)
    processed_data = []

    for result in results[:5]:  # Limit to first 5 results per pattern
        try:
            data = {
                'url': result['url'],
                'domain': result['url'].split('/')[2],
                'timestamp': result['timestamp'],
                'filename': result.get('filename'),
                'offset': result.get('offset'),
                'length': result.get('length')
            }

            if all(key in result for key in ['filename', 'offset', 'length']):
                content = api.fetch_page_content(result['filename'], 
                                                 result['offset'], 
                                                 result['length'])
                data['content'] = content

            processed_data.append(data)
            logging.debug(f"Processed data for URL: {data['url']}")
        except Exception as e:
            logging.error(f"Error processing result {result}: {e}")
    logging.info(f"Processed {len(processed_data)} items for pattern: {pattern}")
    return processed_data

def store_in_database(data):
    if not data:
        logging.warning("No data to store in database.")
        return

    try:
        conn = sqlite3.connect('financial_urls.db')
        cursor = conn.cursor()
        logging.debug("Connected to financial_urls.db")
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_urls (
                url TEXT PRIMARY KEY,
                domain TEXT,
                timestamp TEXT,
                filename TEXT,
                offset INTEGER,
                length INTEGER,
                content TEXT
            )
        ''')
        logging.debug("Ensured financial_urls table exists.")
        
        for item in data:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO financial_urls 
                    (url, domain, timestamp, filename, offset, length, content)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    item['url'],
                    item['domain'],
                    item['timestamp'],
                    item.get('filename'),
                    item.get('offset'),
                    item.get('length'),
                    item.get('content')
                ))
                logging.debug(f"Inserted/Updated URL: {item['url']}")
            except sqlite3.IntegrityError as e:
                logging.error(f"Integrity error for URL {item['url']}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error inserting URL {item['url']}: {e}")
        
        conn.commit()
        logging.info("Committed all changes to the database.")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        conn.close()
        logging.debug("Closed database connection.")
    
def get_commoncrawl_urls():
    api = CommonCrawlAPI()
    domains = get_financial_domains()
    logging.info(f"Searching for {len(domains)} domains")
    
    total_urls = 0
    
    # Reduced the number of max_workers to 2 to lessen the load on the API
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_pattern = {executor.submit(process_pattern, api, pattern): pattern for pattern in domains}
            for future in concurrent.futures.as_completed(future_to_pattern):
                pattern = future_to_pattern[future]
                try:
                    pattern_results = future.result()
                    store_in_database(pattern_results)
                    total_urls += len(pattern_results)
                    logging.info(f"Processed pattern: {pattern} with {len(pattern_results)} URLs")
                except Exception as e:
                    logging.error(f"Error processing pattern {pattern}: {e}")
                
                # Rate limiting to prevent overwhelming the API
                time.sleep(2)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Shutting down gracefully.")
    except Exception as e:
        logging.error(f"Error during parallel processing: {e}")
    
    logging.info(f"\nTotal URLs found: {total_urls}")
    return total_urls

def query_database(domain=None, start_date=None, end_date=None, limit=100):
    """Query the database with optional filters"""
    try:
        conn = sqlite3.connect('financial_urls.db')
        cursor = conn.cursor()
        logging.debug("Connected to financial_urls.db for querying.")
        
        query = "SELECT * FROM financial_urls WHERE 1=1"
        params = []
        
        if domain:
            query += " AND domain = ?"
            params.append(domain)
            logging.debug(f"Filtering by domain: {domain}")
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date)
            logging.debug(f"Filtering by start_date: {start_date}")
            
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date)
            logging.debug(f"Filtering by end_date: {end_date}")
            
        query += f" LIMIT {limit}"
        logging.debug(f"Executing query: {query} with params: {params}")
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        logging.info(f"Query returned {len(results)} results.")
        return results
    except sqlite3.Error as e:
        logging.error(f"Database error during query: {e}")
        return []
    finally:
        conn.close()
        logging.debug("Closed database connection after querying.")

if __name__ == '__main__':
    try:
        total_urls = get_commoncrawl_urls()
        logging.info(f"Completed processing with {total_urls} total URLs found")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
