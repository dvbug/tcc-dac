# coding: utf-8
from flask import Flask, url_for
from flask_restful import Api
from .common.util import JSONEncoder
from .resources.schedule import Schedule, ScheduleList
from .resources.line_config import LineConfig
from . import config


def create_app(config_override=None):
    app = Flask(__name__)
    app.json_encoder = JSONEncoder
    app.config.from_object(config)
    app.config.from_object(config_override)

    api = Api(app, prefix='/api/v1', catch_all_404s=True)

    # TODO register add_resource here
    api.add_resource(ScheduleList,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>',
                     'schedules')
    api.add_resource(Schedule,
                     '/schedules/<string:date>/<string:line_no>/<string:plan_or_real>/<trip>',
                     'schedules')
    api.add_resource(LineConfig, '/config/lines/<string:line_no>',
                     'line')
    return app



# TODO need to split all the Resouce data single
