import json
import os
from flask import jsonify
from flask_restx import Resource, Namespace, fields

ns = Namespace('news', description='Stock news data and inflection points.')

# Define the nested models for the API documentation
news_article_model = ns.model('NewsArticle', {
    'id': fields.String(description='News article ID'),
    'title': fields.String(description='Article title'),
    'publishedDate': fields.String(description='Publication date'),
    'source': fields.String(description='News source'),
    'url': fields.String(description='Article URL'),
    'type': fields.String(description='Article type')
})

company_model = ns.model('Company', {
    'ticker': fields.String(description='Company ticker symbol'),
    'name': fields.String(description='Company name'),
    'exchange': fields.String(description='Stock exchange')
})

market_event_model = ns.model('MarketEvent', {
    'type': fields.String(description='Event type'),
    'key_points': fields.List(fields.String, description='Key points from the event'),
    'major_shareholders': fields.List(fields.String, description='Major shareholders involved')
})

analysis_model = ns.model('Analysis', {
    'key_findings': fields.List(fields.String, description='Key findings from analysis'),
    'sentiment': fields.String(description='Sentiment analysis'),
    'risk_factors': fields.List(fields.String, description='Risk factors identified')
})

event_classification_model = ns.model('EventClassification', {
    'primary_type': fields.String(description='Primary event type'),
    'sub_type': fields.String(description='Event sub-type'),
    'severity': fields.Integer(description='Event severity'),
    'confidence': fields.Float(description='Confidence score'),
    'impact_duration': fields.String(description='Duration of impact')
})

inflection_model = ns.model('Inflection', {
    'date': fields.String(description='Inflection date'),
    'price': fields.Float(description='Stock price'),
    'index': fields.Integer(description='Index'),
    'prev_date': fields.String(description='Previous date'),
    'prev_price': fields.Float(description='Previous price'),
    'price_change': fields.Float(description='Price change')
})

@ns.route('/by-inflection/<string:symbol>')
class NewsByInflection(Resource):
    @ns.doc('get_news_by_inflection',
            params={'symbol': 'Stock symbol (e.g., OXY)'},
            responses={
                200: 'Success',
                404: 'Data not found',
                500: 'Internal server error'
            })
    def get(self, symbol):
        """Get news data organized by inflection points for a given stock symbol"""
        try:
            # Construct the file path
            file_path = os.path.join(
                os.path.dirname(__file__), 
                '..', 'data', 
                f'news_by_inflection_{symbol}.json'
            )
            
            # Check if file exists
            if not os.path.exists(file_path):
                return {'error': f'No news data found for symbol {symbol}'}, 404
            
            # Load and return the JSON data
            with open(file_path, 'r') as file:
                data = json.load(file)
                return jsonify(data)
                
        except Exception as e:
            return {'error': f'Error loading news data: {str(e)}'}, 500 