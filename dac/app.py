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
from .common.util import JSONEncoder
from .resources.schedule import Schedule, ScheduleList
from .resources.line_config import LineConfig, LineConfigList
from .config import API_VERSION


def create_app(config_override=None):
    _api_version_prefix = '/api/{}'.format(API_VERSION).rstrip('/')
    app = Flask(__name__)
    app.json_encoder = JSONEncoder
    app.config.from_object(config)
    app.config.from_object(config_override)

    FlaskJSON(app)

    api = Api(app, prefix=_api_version_prefix, catch_all_404s=True)

    # TODO register more resources here ...
    api.add_resource(ScheduleList,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>',
                     endpoint='schedule.list')
    api.add_resource(Schedule,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>/<string:trip>',
                     endpoint='schedule.trip')
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
        apis = [str(rule) for rule in app.url_map._rules if str(rule).startswith(api.prefix)]
        apis.sort(key=lambda r: r)
        resp_data = {
            'apis': apis,
            'status': 200,
            'message': 'Resources list see apis.',
            'version': API_VERSION
        }
        return jsonify(resp_data), 200


# TODO need to split all the Resouce data single - NOT DO
