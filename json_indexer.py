import json
from datetime import datetime
from collections import defaultdict

def load_json_file(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def parse_date(date_str):
    return datetime.strptime(date_str[:10], '%Y-%m-%d')

def find_closest_inflection(news_date, inflection_points):
    news_dt = parse_date(news_date)
    min_diff = float('inf')
    closest_point = None
    
    for point in inflection_points:
        inflection_dt = parse_date(point['date'])
        diff = abs((news_dt - inflection_dt).days)
        if diff < min_diff:
            min_diff = diff
            closest_point = point
            
    return closest_point

def index_news_by_inflection():
    # Load data
    articles = load_json_file('gm_single_20241129_235109/filtered_articles.json')
    inflection_points = load_json_file('inflection_points_AAPL.json')
    
    # Create index
    news_by_inflection = defaultdict(lambda: {'news': [], 'inflection': None})
    
    # Index each news article
    for article in articles:
        news_date = article['news_article']['publishedDate']
        closest_point = find_closest_inflection(news_date, inflection_points)
        
        if closest_point:
            inflection_date = closest_point['date']
            news_by_inflection[inflection_date]['news'].append(article)
            if not news_by_inflection[inflection_date]['inflection']:
                news_by_inflection[inflection_date]['inflection'] = closest_point
    
    # Convert to regular dict for JSON serialization
    result = dict(news_by_inflection)
    
    # Save indexed data
    with open('news_by_inflection.json', 'w') as f:
        json.dump(result, f, indent=2)
        
    return result

if __name__ == "__main__":
    index_news_by_inflection()