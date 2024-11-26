import json
import asyncio
from bs4 import BeautifulSoup
import httpx

async def fetch_content(client, url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    try:
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            # Try different encodings
            encodings = ['utf-8', 'iso-8859-1', 'windows-1252']
            content = None
            for encoding in encodings:
                try:
                    content = response.content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                return {
                    'status': 'error',
                    'error': 'Unable to decode content',
                    'status_code': response.status_code
                }
            
            soup = BeautifulSoup(content, 'html.parser')
            return {
                'status': 'success',
                'content': soup.get_text(),
                'status_code': response.status_code
            }
        else:
            return {
                'status': 'error',
                'error': f'HTTP {response.status_code}',
                'status_code': response.status_code
            }
    except httpx.RequestError as e:
        return {
            'status': 'error',
            'error': str(e),
            'status_code': None
        }

async def download_articles(urls):
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    transport = httpx.AsyncHTTPTransport(limits=limits)
    async with httpx.AsyncClient(transport=transport, timeout=30.0) as client:
        tasks = [fetch_content(client, url) for url in urls]
        contents = await asyncio.gather(*tasks)
    return contents

# Load the GDELT articles from JSON file
with open('gdelt_articles_with_content.json', 'r') as f:
    articles = json.load(f)

urls = [article['url'] for article in articles]

# Fetch content for each URL
contents = asyncio.run(download_articles(urls))

results_data = {
    'successful_articles': [],
    'failed_articles': [],
    'summary': {
        'total_urls': len(urls),
        'successful': 0,
        '404_errors': 0,
        '403_errors': 0,
        'other_errors': 0
    }
}

print("\nFetching Results:")
for article, result in zip(articles, contents):
    article_data = {
        'url': article['url'],
        'timestamp': article.get('timestamp', None)
    }
    
    if result['status'] == 'success':
        results_data['summary']['successful'] += 1
        article_data.update({
            'content': result['content'],
            'status_code': result['status_code']
        })
        results_data['successful_articles'].append(article_data)
        print(f"\nSuccess - URL: {article['url']}")
        print(f"Content preview: {result['content'][:200]}...")
    else:
        status_code = result['status_code']
        if status_code == 404:
            results_data['summary']['404_errors'] += 1
        elif status_code == 403:
            results_data['summary']['403_errors'] += 1
        else:
            results_data['summary']['other_errors'] += 1
            
        article_data.update({
            'error': result['error'],
            'status_code': status_code
        })
        results_data['failed_articles'].append(article_data)
        print(f"\nFailed - URL: {article['url']}")
        print(f"Error: {result['error']}")

output_filename = 'scraped_articles_results.json'
with open(output_filename, 'w', encoding='utf-8') as f:
    json.dump(results_data, f, ensure_ascii=False, indent=2)

print("\nSummary:")
print(f"Successful requests: {results_data['summary']['successful']}")
print(f"404 errors: {results_data['summary']['404_errors']}")
print(f"403 errors: {results_data['summary']['403_errors']}")
print(f"Other errors: {results_data['summary']['other_errors']}")
print(f"\nResults saved to: {output_filename}")
