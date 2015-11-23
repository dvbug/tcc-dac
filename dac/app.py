# coding: utf-8
from flask import Flask, url_for
from flask_restful import Api
from .common.util import JSONEncoder
from .resources.schedule import Schedule, ScheduleList
from .resources.line_config import LineConfig
from . import config
from .config import API_VERSION


def create_app(config_override=None):
    _api_version_prefix = '/api/{}'.format(API_VERSION).rstrip('/')
    app = Flask(__name__)
    app.json_encoder = JSONEncoder
    app.config.from_object(config)
    app.config.from_object(config_override)

    api = Api(app, prefix=_api_version_prefix, catch_all_404s=True)

    # TODO register add_resource here
    api.add_resource(ScheduleList,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>',
                     endpoint='schedule.list')
    api.add_resource(Schedule,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>/<string:trip>',
                     endpoint='schedule.trip')
    api.add_resource(LineConfig, '/configs/lines/<string:line_no>',
                     endpoint='configs.lines')

    init_app_index(app, api)

    return app


def init_app_index(app: Flask, api: Api):
    from flask import jsonify

    @app.route('/')
    def index():
        resp_data = {
            'apis': [str(rule) for rule in app.url_map._rules if str(rule).startswith(api.prefix)],
            'status': 200,
            'message': 'Resources list see apis.',
            'version': API_VERSION
        }
        return jsonify(resp_data), 200


# TODO need to split all the Resouce data single
