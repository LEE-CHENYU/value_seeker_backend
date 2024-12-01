from gdeltdoc import GdeltDoc, Filters
import json
import aiohttp
import asyncio
from bs4 import BeautifulSoup

class GDELTNewsFetcher:
    def __init__(self, keywords, themes, domains, start_date, end_date, num_articles=5):
        self.filters = Filters(
            keyword=keywords,
            theme=themes,
            domain=domains,
            start_date=start_date,
            end_date=end_date
        )
        self.gd = GdeltDoc()
        self.articles = None
        self.articles_dict = None
        self.num_articles = num_articles

    def fetch_articles(self):
        self.articles = self.gd.article_search(self.filters)
        print(self.articles)
        self.articles_dict = self.articles.to_dict(orient='records')
        if self.num_articles:
            self.articles_dict = self.articles_dict[:self.num_articles]

    async def fetch_content(self, session, url):
        async with session.get(url) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                return soup.get_text()
        return None

    async def download_articles(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_content(session, article['url']) for article in self.articles_dict]
            contents = await asyncio.gather(*tasks)
            
        for article, content in zip(self.articles_dict, contents):
            article['content'] = content

    async def run(self):
        self.fetch_articles()
        await self.download_articles()
        
        # Save to JSON file with content
        with open('gdelt_articles_with_content.json', 'w') as f:
            json.dump(self.articles_dict, f, indent=4)

if __name__ == "__main__":
    fetcher = GDELTNewsFetcher(
        keywords=["apple", "AAPL"],
        themes=["ECON_STOCKMARKET", "ECON_TRADE"],
        domains=["cnbc.com", "businessinsider.com", "seekingalpha.com", "investing.com", "finance.yahoo.com", "marketwatch.com", "morningstar.com"],
        start_date="2023-10-14",
        end_date="2023-11-01",
        num_articles=50  # Specify the number of articles you want
    )
    asyncio.run(fetcher.run())
