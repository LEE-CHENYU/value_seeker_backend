import json
import asyncio
from bs4 import BeautifulSoup
import httpx
import io
from pdfminer.high_level import extract_text
import traceback
from httpx import AsyncClient, RequestError

async def fetch_content(semaphore, client, url, retries=3):
    async with semaphore:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'identity',
            'Connection': 'keep-alive',
        }
        for attempt in range(retries):
            try:
                response = await client.get(url, headers=headers)
                content_type = response.headers.get('Content-Type', '')
                if response.status_code == 200:
                    if 'application/pdf' in content_type.lower():
                        pdf_bytes = response.content
                        pdf_stream = io.BytesIO(pdf_bytes)
                        pdf_text = extract_text(pdf_stream)
                        return {
                            'status': 'success',
                            'content': pdf_text,
                            'status_code': response.status_code
                        }
                    elif 'text/html' in content_type.lower():
                        encoding = response.encoding if response.encoding else 'utf-8'
                        response.encoding = encoding
                        content = response.text
                        soup = BeautifulSoup(content, 'html.parser')
                        text_content = soup.get_text()
                        return {
                            'status': 'success',
                            'content': text_content,
                            'status_code': response.status_code
                        }
                    else:
                        return {
                            'status': 'error',
                            'error': f'Unsupported Content-Type: {content_type}',
                            'status_code': response.status_code
                        }
                else:
                    return {
                        'status': 'error',
                        'error': f'HTTP {response.status_code}',
                        'status_code': response.status_code
                    }
            except RequestError as e:
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    error_message = f'Failed after {retries} attempts: {str(e)}'
                    return {
                        'status': 'error',
                        'error': error_message,
                        'status_code': None
                    }

async def download_articles(urls):
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
    semaphore = asyncio.Semaphore(10)
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        tasks = [fetch_content(semaphore, client, url) for url in urls]
        contents = await asyncio.gather(*tasks)
    return contents

# Load the articles from JSON file
with open('gdelt_articles_with_content.json', 'r', encoding='utf-8') as f:
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
        status_code = result.get('status_code')
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
