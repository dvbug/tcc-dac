# coding: utf-8
"""
    tcc-dac.dac.app
    ~~~~~~~~~~~~~~~

    tcc-dac dac app factory module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from flask import Flask
from flask_json import FlaskJSON
from flask_restful import Api
from . import config
from dac.common.util import JSONEncoder
from dac.resources.schedule import Schedule, ScheduleList, DateScheduleList
from dac.resources.line_config import LineConfig, LineConfigList
from dac.data_center.database import db
from dac.data_center.cache import redis_client


def create_app(config_override=None):

    app = Flask(__name__)
    app.json_encoder = JSONEncoder
    app.config.from_object(config)
    app.config.from_object(config_override)

    FlaskJSON(app)

    _api_version_prefix = '/api/{}'.format(app.config['API_VERSION'] or 'v1.0').rstrip('/')
    api = Api(app, prefix=_api_version_prefix, catch_all_404s=True)

    db.init_app(app)

    redis_client.init_app(app)

    # TODO register more resources here ...

    api.add_resource(DateScheduleList,
                     '/schedules/<string:date>/<string:plan_or_real>',
                     endpoint='schedule.all.list')
    api.add_resource(ScheduleList,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>',
                     endpoint='schedule.line.list')
    api.add_resource(Schedule,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>/<string:trip>',
                     endpoint='schedule.line.trip')
    api.add_resource(LineConfigList, '/configs/lines',
                     endpoint='configs.lines')
    api.add_resource(LineConfig, '/configs/lines/<string:line_no>',
                     endpoint='configs.line')

    init_app_index(app, api)

    return app


def init_app_index(app: Flask, api: Api):
    """Init flask app's '/' route. Returns all registered resources."""
    from flask import jsonify

    @app.route('/')
    def index():
        apis = [{'url': str(rule), 'methods': ','.join(rule.methods), 'endpoint': str(rule.endpoint)}
                for rule in app.url_map._rules if str(rule).startswith(api.prefix)]
        apis.sort(key=lambda d: d['url'])
        resp_data = {
            'apis': apis,
            'status': 200,
            'message': 'Resources list see apis.',
            'version': app.config['API_VERSION'] or 'v1.0'
        }
        return jsonify(resp_data), 200

