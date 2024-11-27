import os
import sys

# parent_dir = os.path.dirname(os.getcwd())
# sys.path.append(parent_dir)

import logging 
from flask_stock import create_app
from flask_stock.flask_settings import * 
from flask_stock.controllers.health import ns as health_ns
from flask_stock.controllers.kline_data import ns as kline_ns
from flask import jsonify
logger = logging.getLogger(__name__)

app, api = create_app()

api.add_namespace(health_ns)
api.add_namespace(kline_ns)
logger.debug(f"api: {api.namespaces}")

logger.debug(f"flask_app.blueprint: {app.blueprints.__repr__()}")

@app.route('/test')
def test():
    return jsonify({"message": "Hello, World!"})

if __name__ == '__main__':
    logger.debug("Starting Flask app")

    app.run(port=8080)

    print(app.config)