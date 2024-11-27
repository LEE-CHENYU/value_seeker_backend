import os
import sys

# parent_dir = os.path.dirname(os.getcwd())
# sys.path.append(parent_dir)

import logging 
from flask_stock import create_app
from flask_stock.flask_settings import * 
from flask_stock.controllers.health import ns as health_ns
from flask_stock.controllers.kline_data import ns as kline_ns
from flask import jsonify, request
from flask_cors import CORS  # You'll need to install flask-cors

logger = logging.getLogger(__name__)

app, api = create_app()
CORS(app)  # Enable CORS for all routes

api.add_namespace(health_ns)
api.add_namespace(kline_ns)
logger.debug(f"api: {api.namespaces}")

logger.debug(f"flask_app.blueprint: {app.blueprints.__repr__()}")

@app.route('/test')
def test():
    return jsonify({"message": "Hello, World!"})

@app.route('/api/kline')
def get_kline_data():
    symbol = request.args.get('symbol', default='AAPL')  # Default to AAPL if no symbol provided
    interval = request.args.get('interval', default='1d')  # Default to daily data
    
    try:
        # Import your kline data service/controller
        from flask_stock.services.kline_service import get_stock_kline_data
        
        kline_data = get_stock_kline_data(symbol, interval)
        return jsonify({
            'success': True,
            'data': kline_data,
            'symbol': symbol
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    logger.debug("Starting Flask app")

    app.run(port=8080)

    print(app.config)