import os
import sys
# Flask: 这是Flask框架的核心类，用于创建Web应用程序实例
from flask import Flask

# Blueprint(蓝图): Flask的一个重要功能，它允许你将应用程序组织成一组相关的视图、模板和静态文件
# 主要用途:
# - 将大型应用拆分成多个模块，便于管理
# - 在不同的URL前缀下注册视图函数
# - 定义模块特定的模板和静态文件
# - 实现可重用的应用组件
from flask import Blueprint

# Api: Flask-RESTX提供的主要类，用于构建RESTful API，提
# 供自动API文档等功能
from flask_restx import Api

# ProxyFix: 一个WSGI中间件，用于处理代理服务器的情况，确保请求头信息正确传递
from werkzeug.middleware.proxy_fix import ProxyFix
# parent_dir = os.path.dirname(os.getcwd())
# sys.path.append(parent_dir)
from . import flask_settings
global app
api = Api(version='1.0', title='Stock API', description='API for stock data')

global frequency
global bxp_family_id
global bxp_app_id
global bxp_service_id
global bxp_service_deployment_id
global bxp_environment
global app_context 
global app_name

from .controllers.kline_data import ns as kline_ns
from flask_cors import CORS

def create_app():
    print("Creating app")
    global app
    app = Flask(__name__)
    CORS(app)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1) 
    app.debug = flask_settings.FLASK_DEBUG
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    app.config['PROPAGATE_EXCEPTIONS'] = True
    app.config['RESTPLUS_VALIDATE'] = flask_settings.RESTPLUS_VALIDATE
    app.config['RESTPLUS_MASK_SWAGGER'] = flask_settings.RESTPLUS_MASK_SWAGGER
    app.config['ERROR_404_HELP'] = flask_settings.RESTPLUS_ERROR_404_HELP     

    blueprint = Blueprint(flask_settings.APP_API_CONTEXT, __name__, url_prefix=flask_settings.FLASK_APP_CONTEXT)
    print('blueprint', blueprint.__repr__())
    api.init_app(blueprint, doc=flask_settings.FLASK_APP_CONTEXT)
    app.register_blueprint(blueprint)
    api.add_namespace(kline_ns, path='/api/v1')
    return app, api


if __name__ == '__main__':
    app, api = create_app()
    app.run(debug=flask_settings.FLASK_DEBUG)

