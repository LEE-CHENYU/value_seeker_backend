import time
import os
import sys
import json
import requests
import datetime 
import traceback 
import logging 

from flask import request, Response
from flask_restx import Resource, fields, Namespace

from werkzeug.datastructures import FileStorage

ns = Namespace('kline', description='Extracted kline data and turning points.')

request_parser = ns.parser()
request_parser.add_argument('symbol', type=str, required=True, help='The symbol of the stock')
request_parser.add_argument('api_key', type=str, required=True, help='The API key for the stock data')
request_parser.add_argument('function', type=str, required=False, help='The function to use for the stock data')
request_parser.add_argument('years_to_display', type=int, required=False, help='The number of years to display')
request_parser.add_argument('significant_change_threshold', type=float, required=False, help='The significant change threshold')

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
    'Meta Data': fields.Nested(meta_data_model),
    'Monthly Time Series': fields.Raw(description='Time series data')  # 使用Raw因为键是动态的日期
})

@ns.route('/kline-raw')
class KlineRaw(Resource):
    @ns.expect(request_parser)
    @ns.marshal_with(kline_response_model)
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
        function = args['function']
        years_to_display = args['years_to_display']
        significant_change_threshold = args['significant_change_threshold']

        request_url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}"
        data = requests.get(request_url)
        return data.json()

