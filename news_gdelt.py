from gdeltdoc import GdeltDoc, Filters
import json
import aiohttp
import asyncio
from bs4 import BeautifulSoup

f = Filters(
    keyword = ["apple", "APPL"],
    theme = ["ECON_STOCKMARKET", "ECON_TRADE"],
    domain = ["cnbc.com", "businessinsider.com", "seekingalpha.com", "investing.com", "finance.yahoo.com", "marketwatch.com", "morningstar.com"],
    # domain = ["investing.com", "seekingalpha.com", "finance.yahoo.com", "marketwatch.com"],
    start_date = "2022-10-14",
    end_date = "2024-11-15"
)

gd = GdeltDoc()
articles = gd.article_search(f)

print(articles)

# Convert articles DataFrame to dictionary
articles_dict = articles.to_dict(orient='records')

async def fetch_content(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            return soup.get_text()
    return None

async def download_articles(articles):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_content(session, article['url']) for article in articles]
        contents = await asyncio.gather(*tasks)
        
    for article, content in zip(articles, contents):
        article['content'] = content

async def main():
    await download_articles(articles_dict)
    
    # Save to JSON file with content
    with open('gdelt_articles_with_content.json', 'w') as f:
        json.dump(articles_dict, f, indent=4)

asyncio.run(main())
