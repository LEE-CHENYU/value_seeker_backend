import time
import os
import sys
import json
import requests


import datetime 
import traceback 
import logging 

from flask import request, Response, jsonify
from flask_restx import Resource, fields, Namespace

from werkzeug.datastructures import FileStorage

from ..utils.k_line import KLine

ns = Namespace('kline', description='Extracted kline data and turning points.')

request_parser = ns.parser()
request_parser.add_argument('symbol', type=str, required=True, help='The symbol of the stock')
request_parser.add_argument('api_key', type=str, required=True, help='The API key for the stock data')
request_parser.add_argument('function', type=str, required=False, help='The function to use for the stock data')
request_parser.add_argument('years_to_display', type=int, required=False, help='The number of years to display')
request_parser.add_argument('significant_change_threshold', type=float, required=False, help='The significant change threshold')


class KLineResponseFormat:
    time_series_model = ns.model('TimeSeriesData', {
        '1. open': fields.String(description='Opening price'),
        '2. high': fields.String(description='Highest price'),
        '3. low': fields.String(description='Lowest price'),
        '4. close': fields.String(description='Closing price'),
        '5. volume': fields.String(description='Trading volume')
})

# 定义元数据模型
    meta_data_model = ns.model('MetaData', {
        '1. Information': fields.String(description='Information about the data'),
        '2. Symbol': fields.String(description='Stock symbol'),
        '3. Last Refreshed': fields.String(description='Last update time'),
        '4. Time Zone': fields.String(description='Time zone of the data')
})

# 定义完整响应模型
    kline_response_model = ns.model('KlineResponse', {
        'Meta Data': fields.Raw(description='Meta data'),
        'Monthly Time Series': fields.Raw(description='Time series data')  # 使用Raw因为键是动态的日期
})

@ns.route('/kline-raw')
class KlineRaw(Resource):
    @ns.marshal_with(KLineResponseFormat.kline_response_model)
    @ns.expect(request_parser)
    @ns.response(200, 'Success')
    @ns.response(400, 'Bad Request')
    @ns.response(500, 'Internal Server Error')
    @ns.response(404, 'Not Found')
    @ns.response(429, 'Too Many Requests')
    @ns.response(403, 'Forbidden')
    @ns.response(410, 'Gone')
    @ns.response(401, 'Unauthorized')
    @ns.response(409, 'Conflict')
    @ns.response(412, 'Precondition Failed')
    @ns.response(422, 'Unprocessable Entity')
    @ns.response(428, 'Precondition Required')
    @ns.response(413, 'Payload Too Large')
    @ns.response(425, 'Too Early')
    @ns.response(451, 'Unavailable For Legal Reasons')
    @ns.response(503, 'Service Unavailable')
    @ns.response(504, 'Gateway Timeout')
    @ns.response(505, 'HTTP Version Not Supported')
    @ns.response(501, 'Not Implemented')
    @ns.doc(description='Get the raw kline data from the Alpha Vantage API.')
    def post(self):
        args = request_parser.parse_args()
        symbol = args['symbol']
        api_key = args['api_key']
        function = args['function'] if args['function'] else 'TIME_SERIES_MONTHLY'
        years_to_display = args['years_to_display'] if args['years_to_display'] else 20
        significant_change_threshold = args['significant_change_threshold'] if args['significant_change_threshold'] else 0.05

        request_url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}"
        response = requests.get(request_url)
        data = response.json()
        # data = json.load(open('kline_TIME_SERIES_MONTHLY_OXY.json'))
        try:
            print("Raw API response:", data.get('Meta Data'))  # 调试用
            meta_data = data.get('Meta Data')
            time_series = data.get('Monthly Time Series')
            
                        # 构建响应数据
            response_data = {
                'Meta Data': meta_data,
                'Monthly Time Series': time_series
            }
            print(response_data)
            # 手动验证和序列化
            return response_data
            
        except KeyError as e:
            print(f"Error processing data: {e}")  # 调试用
            ns.abort(500, f"Unexpected API response format: {str(e)}")

@ns.route('/inflection-points/<string:symbol>')
class InflectionPoints(Resource):
    @ns.doc('get_inflection_points')
    @ns.param('api_key', 'Alpha Vantage API key', required=True)
    @ns.param('threshold', 'Significant change threshold (default: 0.1)', required=False)
    @ns.param('years', 'Number of years to analyze (default: 20)', required=False)
    def get(self, symbol):
        """Get significant inflection points for a stock"""
        args = request.args
        api_key = args.get('api_key')
        threshold = float(args.get('threshold', 0.1))
        years = int(args.get('years', 20))

        if not api_key:
            return {'error': 'API key is required'}, 400

        try:
            kline = KLine(
                symbol=symbol,
                api_key=api_key,
                years_to_display=years,
                significant_change_threshold=threshold
            )
            inflection_points = kline.get_inflection_points()
            return jsonify(inflection_points)
        except Exception as e:
            return {'error': str(e)}, 500

if __name__ == '__main__':
    print(KLineResponseFormat.kline_response_model)