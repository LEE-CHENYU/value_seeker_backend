import json
from flask import Response
from flask_restx import Resource, Namespace


ns = Namespace('health', description='Health check')

@ns.route('/')
class Health(Resource):
    def get(self):
        return Response(json.dumps({'status': 'UP'}))
