import json
from datetime import datetime
from collections import defaultdict

class NewsIndexer:
    def __init__(self, articles_file, inflection_points_file, output_file):
        self.articles_file = articles_file
        self.inflection_points_file = inflection_points_file
        self.output_file = output_file
        self.articles = None
        self.inflection_points = None
        self.news_by_inflection = defaultdict(lambda: {'news': [], 'inflection': None})

    @staticmethod
    def load_json_file(filename):
        with open(filename, 'r') as f:
            return json.load(f)

    @staticmethod
    def parse_date(date_str):
        return datetime.strptime(date_str[:10], '%Y-%m-%d')

    def find_closest_inflection(self, news_date):
        news_dt = self.parse_date(news_date)
        min_diff = float('inf')
        closest_point = None
        
        for point in self.inflection_points:
            inflection_dt = self.parse_date(point['date'])
            diff = abs((news_dt - inflection_dt).days)
            if diff < min_diff:
                min_diff = diff
                closest_point = point
                
        return closest_point

    def load_data(self):
        self.articles = self.load_json_file(self.articles_file)
        self.inflection_points = self.load_json_file(self.inflection_points_file)

    def index_news(self):
        for article in self.articles:
            news_date = article['news_article']['publishedDate']
            closest_point = self.find_closest_inflection(news_date)
            
            if closest_point:
                inflection_date = closest_point['date']
                self.news_by_inflection[inflection_date]['news'].append(article)
                if not self.news_by_inflection[inflection_date]['inflection']:
                    self.news_by_inflection[inflection_date]['inflection'] = closest_point

    def save_indexed_data(self):
        result = dict(self.news_by_inflection)
        with open(self.output_file, 'w') as f:
            json.dump(result, f, indent=2)
        return result

    def run(self):
        self.load_data()
        self.index_news()
        return self.save_indexed_data()

if __name__ == "__main__":
    indexer = NewsIndexer(
        'gm_single_20241129_235109/filtered_articles.json',
        'inflection_points_AAPL.json',
        'news_by_inflection.json'
    )
    indexer.run()