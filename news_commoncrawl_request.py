import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

def simple_commoncrawl_request():
    # Latest CommonCrawl index as of 2023
    index = "CC-MAIN-2023-50"
    
    # API endpoint
    api_url = f"https://index.commoncrawl.org/{index}-index"
    
    # Search parameters
    params = {
        'url': 'finance.yahoo.com/quote/*',
        'output': 'json'
    }
    
    try:
        logging.info("Sending request to CommonCrawl API...")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        # Parse results
        results = [json.loads(line) for line in response.text.strip().split('\n') if line]
        
        # Take first result only
        if results:
            first_result = results[0]
            logging.info(f"Found URL: {first_result['url']}")
            logging.info(f"Timestamp: {first_result['timestamp']}")
            return first_result
        else:
            logging.info("No results found")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making request: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing response: {e}")
        return None

if __name__ == '__main__':
    simple_commoncrawl_request()


